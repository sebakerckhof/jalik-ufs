const optionsSchema = new SimpleSchema({
    adaptive:{
        type:String,
        optional:true,
        defaultValue:true
    },
    capacity: {
        type: Number,
        decimal:true,
        optional: true,
        defaultValue: 0.9,
        min:0,
        max:1,
    },
    chunkSize:{
        type:Number,
        defaultValue:8 * 1024,
    },
    data: {
        type: ArrayBuffer
    },
    file:{
        type: Object,
        optional:true
    },
    maxChunkSize:{
        type:Number,
        optional:true,
        defaultValue:0
    },
    maxTries:{
        type:Number,
        optional:true,
        defaultValue:5
    },
    onAbort:{
        type:Function,
        optional:true
    },
    onComplete:{
        type:Function,
        optional:true
    },
    onCreate:{
        type:Function,
        optional:true
    },
    onError:{
        type:Function,
        optional:true
    },
    onProgress:{
        type:Function,
        optional:true
    },
    onStart:{
        type:Function,
        optional:true
    },
    onStop:{
        type:Function,
        optional:true
    },
    store:{
        type:String
    }
});

/**
 * File uploader
 * @param options
 * @constructor
 */
class Uploader{
    constructor(options){
        optionsSchema.clean(options);
        check(options,optionsSchema);
        
        Object.assign(this,options);

        // Private attributes
        this._store = options.store;
        this._data = options.data;
        this._capacityMargin = 10; //%
        this._file = options.file;
        this._fileId = null;
        this._offset = 0;
        this._total = options.data.byteLength;
        this._tries = 0;

        this._complete = new ReactiveVar(false);
        this._loaded = new ReactiveVar(0);
        this._uploading = new ReactiveVar(false);

        this.timeA = null;
        this.timeB = null;
        
        file.store = this._store;
    }

    /**
     * Aborts the current transfer
     */
    abort(){
        this.isUploading = false;

        // Remove the file from database
        store.getCollection().remove(fileId, function (err) {
            if (err) {
                console.error(`ufs: cannot remove file ${ fileId } (${ err.message })`);
            } else {
                this._fileId = null;
                this._offset = 0;
                this._tries = 0;
                this.loaded = 0;
                this.isComplete = false;
                this.onAbort(file);
            }
        });
    };

    /**
     * Returns the file
     * @return {object}
     */
    get file() {
        return this._file;
    }

    /**
     * Returns the loaded bytes
     * @return {number}
     */
    get loaded(){
        return this._loaded.get();
    }

    set loaded(loaded){
        return this._loaded.set(loaded);
    }
    /**
     * Returns current progress
     * @return {number}
     */
    get progress() {
        return parseFloat((this.loaded / this.total).toFixed(2));
    };

    /**
     * Returns the total bytes
     * @return {number}
     */
    get total() {
        return this._total;
    }

    /**
     * Checks if the transfer is complete
     * @return {boolean}
     */
    get isComplete() {
        return this._complete.get();
    }

    set isComplete(isComplete){
        this._complete.set(isComplete);
    }

    /**
     * Checks if the transfer is active
     * @return {boolean}
     */
    get isUploading() {
        return this._uploading.get();
    }

    set isUploading(uploading){
        this._uploading.set(uploading);
    }

    /**
     * Starts or resumes the transfer
     */
    start() {
        if (!this.isUploading && !this.isComplete) {
            this.onStart(file);

            const upload = () => {
                this.isUploading = true;

                let length = this.chunkSize;

                const sendChunk = () => {
                    if (this.isUploading && !this.isComplete) {

                        // Calculate the chunk size
                        if (this._offset + length > this.total) {
                            length = this.total - this._offset;
                        }

                        if (this._offset < this.total) {
                            // Prepare the chunk
                            const chunk = new Uint8Array(data, this._offset, length);
                            const progress = (this._offset + length) / this.total;

                            this.timeA = Date.now();

                            // Write the chunk to the store
                            Meteor.call('ufsWrite', chunk, this._fileId, this._store, progress, (err, bytes) => {
                                this.timeB = Date.now();

                                if (err || !bytes) {
                                    // Retry until max tries is reach
                                    // But don't retry if these errors occur
                                    if (this._tries < this.maxTries && !_.contains([400, 404], err.error)) {
                                        this._tries += 1;

                                        // Wait 1 sec before retrying
                                        Meteor.setTimeout(sendChunk, 1000);

                                    } else {
                                        this.abort();
                                        this.onError(err);
                                    }
                                } else {
                                    this._offset += bytes;
                                    this.loaded = this.loaded + bytes;

                                    // Use adaptive length
                                    if (this.adaptive && this.timeA && this.timeB && this.timeB > this.timeA) {
                                        const duration = (this.timeB - this.timeA) / 1000;

                                        const max = this.capacity * (1 + (this._capacityMargin / 100));
                                        const min = this.capacity * (1 - (this._capacityMargin / 100));

                                        if (duration >= max) {
                                            length = Math.abs(Math.round(bytes * (max - duration)));

                                        } else if (duration < min) {
                                            length = Math.round(bytes * (min / duration));
                                        }
                                        // Limit to max chunk size
                                        if (this.maxChunkSize > 0 && length > this.maxChunkSize) {
                                            length = this.maxChunkSize;
                                        }
                                    }
                                    this.onProgress(file, this.progress);
                                    sendChunk();
                                }
                            });

                        } else {
                            // Finish the upload by telling the store the upload is complete
                            Meteor.call('ufsComplete', this._fileId, this._store, (err, uploadedFile) => {
                                if (err) {
                                    this.abort();
                                } else if (uploadedFile) {
                                    this.isUploading = false;
                                    this.isComplete = true;
                                    this._file = uploadedFile;
                                    this.onProgress(uploadedFile, this.loaded / this.progress);
                                    this.onComplete(uploadedFile);
                                }
                            });
                        }
                    }
                }

                sendChunk();
            }

            if (!this._fileId) {
                // Insert the file in the collection
                store.getCollection().insert(file, (err, uploadId) => {
                    if (err) {
                        this.onError(err);
                    } else {
                        this._fileId = uploadId;
                        this._file._id = this._fileId;
                        this.onCreate(file);
                        upload();
                    }
                });
            } else {
                store.getCollection().update(this._fileId, {
                    $set: {uploading: true}
                }, (err, result) => {
                    if (!err && result) {
                        upload();
                    }
                });
            }
        }
    }

    /**
     * Stops the transfer
     */
    stop() {
        if (this.isUploading) {
            this.isUploading = false;
            store.getCollection().update(this._fileId, {
                $set: {uploading: false}
            });
            this.onStop(file);
        }
    }

    /**
     * Called when the file upload is aborted
     * @param file
     */
    onAbort(file){}

    /**
     * Called when the file upload is complete
     * @param file
     */
    onComplete(file){}

    /**
     * Called when the file is created in the collection
     * @param file
     */
    onCreate(file){}

    /**
     * Called when an error occurs during file upload
     * @param err
     */
    onError(err){
        console.error(err.message);
    }

    /**
     * Called when a file chunk has been sent
     * @param file
     * @param progress is a float from 0.0 to 1.0
     */
    onProgress(file, progress){}

    /**
     * Called when the file upload starts
     * @param file
     */
    onStart(file){}

    /**
     * Called when the file upload stops
     * @param file
     */
    onStop(file){}

}