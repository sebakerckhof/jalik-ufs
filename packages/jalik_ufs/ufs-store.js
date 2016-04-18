const optionsSchema = new SimpleSchema({
    name: {
        type: String,
    },
    collection: {
        type: Mongo.Collection
    },
    filters: {
        type: Array,
        optional:true,
        autoValue:function(){
            if(!this.isSet){
                return [];
            }
        }
    },
    "filters.$":{
        type:UploadFS.Filter,
    },
    "storage":{
        type:Array,
    },
    "storage.$":{
        type:UploadFS.StorageAdapter
    }
});

/**
 * File store
 * @param options
 * @constructor
 */
class Store{
    constructor(options = {}){
        // Set default options
        options = Object.assign({
            collection: null,
            filter: null,
            name: null,
            onCopyError: null,
            onFinishUpload: null,
            onRead: null,
            onReadError: null,
            onWriteError: null,
            transformRead: null,
            transformWrite: null
        }, options);

        optionsSchema.clean(options);
        check(options,optionsSchema);
        
        Object.keys(options).forEach(key => {
            this[`_${key}`] = options[key];
        });

        UploadFS.addStore(this);

        // Code executed before inserting file
        this.collection.before.insert(function (userId, file) {
            if (typeof file.name !== 'string' || !file.name.length) {
                throw new Meteor.Error(400, "file name not defined");
            }
            if (typeof file.store !== 'string' || !file.store.length) {
                throw new Meteor.Error(400, "file store not defined");
            }
            if (typeof file.complete !== 'boolean') {
                file.complete = false;
            }
            if (typeof file.uploading !== 'boolean') {
                file.uploading = true;
            }
            file.extension = file.name && file.name.substr((~-file.name.lastIndexOf('.') >>> 0) + 2).toLowerCase();
            file.progress = parseFloat(file.progress) || 0;
            file.size = parseInt(file.size) || 0;
            file.userId = file.userId || userId;
            file.versions = {};

            this.storage.forEach(storage => {
                file.versions[storage.name] = {
                    processing:false,
                    stored:false
                }
            });
        });

        // Code executed before removing file
        this.collection.before.remove(function (userId, file) {
          this.remove(file);
        });

        this.collection.deny({
            // Test filter on file insertion
            insert: function (userId, file) {
                this.filters.forEach(filter => {
                    filter.check(file);
                });
            }
        });
    }

    remove(file){
        // Delete the physical file in the storages
        this.storage.forEach(storage => {
            if(file.versions.hasOwnProperty(storage.name) && file.versions[storage.name].stored){
                storage.delete(file);
            }
        });

        var tmpFile = UploadFS.getTempFilePath(file._id);

        // Delete the temp file
        fs.stat(tmpFile, function (err) {
            !err && fs.unlink(tmpFile, function (err) {
                err && console.error(`ufs: cannot delete temp file at ${ tmpFile } (${ err.message })`);
            });
        });
    }

    /**
     * Creates the file in the collection
     * @param file
     * @return {string}
     */
    create(file){
        check(file, Object);
        file.store = this.name;
        return this.collection.insert(file);
    }

    /**
     * Returns the collection
     * @return {Mongo.Collection}
     */
    get collection() {
        return this._collection
    }

    /**
     * Returns the file filter
     * @return {UploadFS.Filter}
     */
    get storage() {
        return this._storage;
    }

    /**
     * Returns the file filter
     * @return {UploadFS.Filter}
     */
    get filters() {
        return this._filters;
    }

    /**
     * Returns the store name
     * @return {string}
     */
    get name() {
        return this._name;
    }

    /**
     * Returns the file URL
     * @param fileId
     */
    getFileURL(fileId,version) {
        var file = this.collection.findOne(fileId, {
            fields: {name: 1}
        });
        return file && this.getURL() + '/' + fileId + '/' + encodeURIComponent(file.name);
    }

    /**
     * Returns the store URL
     */
    getURL() {
        return Meteor.absoluteUrl(UploadFS.config.storesPath + '/' + this.name, {
            secure: UploadFS.config.https
        });
    }
}


UploadFS.Store = Store;
function (options) {


    // Public attributes
    self.onCopyError = options.onCopyError || self.onCopyError;
    self.onFinishUpload = options.onFinishUpload || self.onFinishUpload;
    self.onRead = options.onRead || self.onRead;
    self.onReadError = options.onReadError || self.onReadError;
    self.onWriteError = options.onWriteError || self.onWriteError;

    // Private attributes
    var collection = options.collection;
    var copyTo = options.copyTo;
    var filter = options.filter;
    var name = options.name;
    var transformRead = options.transformRead;
    var transformWrite = options.transformWrite;

    // Add the store to the list
    UploadFS.stores[name] = self;




    if (Meteor.isServer) {

        /**
         * Copies the file to a store
         * @param fileId
         * @param store
         * @param callback
         */
        self.copy = function (fileId, store, callback) {
            check(fileId, String);

            if (!(store instanceof UploadFS.Store)) {
                throw new TypeError('store is not an UploadFS.store.Store');
            }

            // Get original file
            var file = self.getCollection().findOne(fileId);
            if (!file) {
                throw new Meteor.Error(404, 'File not found');
            }

            // Prepare copy
            var copy = _.omit(file, '_id', 'url');
            copy.originalStore = self.getName();
            copy.originalId = fileId;

            // Create the copy
            var copyId = store.create(copy);

            // Get original stream
            var rs = self.getReadStream(fileId, file);

            // Catch errors to avoid app crashing
            rs.on('error', Meteor.bindEnvironment(function (error) {
                callback.call(self, error, null);
            }));

            // Copy file data
            store.write(rs, copyId, Meteor.bindEnvironment(function (err) {
                if (err) {
                    store.getCollection().remove(copyId);
                    self.onCopyError.call(self, err, fileId, file);
                }
                if (typeof callback === 'function') {
                    callback.call(self, err, copyId, copy, store);
                }
            }));
        };

     
     

        /**
         * Writes the file to the store
         * @param rs
         * @param fileId
         * @param callback
         */
        self.write = function (rs, fileId, callback) {
            var file = self.getCollection().findOne(fileId);
            var ws = self.getWriteStream(fileId, file);

            var errorHandler = Meteor.bindEnvironment(function (err) {
                self.getCollection().remove(fileId);
                self.onWriteError.call(self, err, fileId, file);
                callback.call(self, err);
            });

            ws.on('error', errorHandler);
            ws.on('finish', Meteor.bindEnvironment(function () {
                var size = 0;
                var readStream = self.getReadStream(fileId, file);

                readStream.on('error', Meteor.bindEnvironment(function (error) {
                    callback.call(self, error, null);
                }));
                readStream.on('data', Meteor.bindEnvironment(function (data) {
                    size += data.length;
                }));
                readStream.on('end', Meteor.bindEnvironment(function () {
                    // Set file attribute
                    file.complete = true;
                    file.progress = 1;
                    file.size = size;
                    file.token = UploadFS.generateToken();
                    file.uploading = false;
                    file.uploadedAt = new Date();
                    file.url = self.getFileURL(fileId);

                    // Sets the file URL when file transfer is complete,
                    // this way, the image will loads entirely.
                    self.getCollection().update(fileId, {
                        $set: {
                            complete: file.complete,
                            progress: file.progress,
                            size: file.size,
                            token: file.token,
                            uploading: file.uploading,
                            uploadedAt: file.uploadedAt,
                            url: file.url
                        }
                    });

                    // Return file info
                    callback.call(self, null, file);

                    // Execute callback
                    if (typeof self.onFinishUpload == 'function') {
                        self.onFinishUpload.call(self, file);
                    }

                    // Simulate write speed
                    if (UploadFS.config.simulateWriteDelay) {
                        Meteor._sleepForMs(UploadFS.config.simulateWriteDelay);
                    }

                    // Copy file to other stores
                    if (copyTo instanceof Array) {
                        for (var i = 0; i < copyTo.length; i += 1) {
                            var store = copyTo[i];

                            if (!store.getFilter() || store.getFilter().isValid(file)) {
                                self.copy(fileId, store);
                            }
                        }
                    }
                }));
            }));

            // Execute transformation
            self.transformWrite(rs, ws, fileId, file);
        };
    }

  
};



if (Meteor.isServer) {
    /**
     * Deletes a file async
     * @param fileId
     * @param callback
     */
    UploadFS.Store.prototype.delete = function (fileId, callback) {
        throw new Error('delete is not implemented');
    };

  
    /**
     * Callback for copy errors
     * @param err
     * @param fileId
     * @param file
     * @return boolean
     */
    UploadFS.Store.prototype.onCopyError = function (err, fileId, file) {
        console.error(`ufs: cannot copy file "${ fileId }" (${ err.message })`);
    };

    /**
     * Called when a file has been uploaded
     * @param file
     */
    UploadFS.Store.prototype.onFinishUpload = function (file) {
    };

    /**
     * Called when a file is read from the store
     * @param fileId
     * @param file
     * @param request
     * @param response
     * @return boolean
     */
    UploadFS.Store.prototype.onRead = function (fileId, file, request, response) {
        return true;
    };

    /**
     * Callback for read errors
     * @param err
     * @param fileId
     * @param file
     * @return boolean
     */
    UploadFS.Store.prototype.onReadError = function (err, fileId, file) {
        console.error('ufs: cannot read file "' + fileId + '" (' + err.message + ')');
    };

    /**
     * Callback for write errors
     * @param err
     * @param fileId
     * @param file
     * @return boolean
     */
    UploadFS.Store.prototype.onWriteError = function (err, fileId, file) {
        console.error('ufs: cannot write file "' + fileId + '" (' + err.message + ')');
    };
}
