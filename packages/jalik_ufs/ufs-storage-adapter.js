const optionsSchema = new SimpleSchema({
  name:{
    type:String,
    optional:true
  },
  transformRead: {
    type: Function,
    optional: true
  },
  transformWrite: {
    type: Function,
    optional: true
  },
});

/**
 * File store
 * @param options
 * @constructor
 */
class StorageAdapter{
  constructor(options = {}){

    // Set default options
    options = Object.assign({
      transformRead: null,
      transformWrite: null
    }, options);

    optionsSchema.clean(options);
    check(options,optionsSchema);

    Object.assign(this,options);
  }

  /**
   * Returns the file URL
   * @param fileId
   */
  getFileURL(fileId) {
    var file = this.collection.findOne(fileId, {
      fields: { name: 1 }
    });
    return file && `${ this.getURL() }/${ fileId }/${ encodeURIComponent(file.name) }`;
  }

  /**
   * Returns the store URL
   */
  getURL() {
    return Meteor.absoluteUrl(`${ UploadFS.config.storesPath }/${ this.name }`, {
      secure: UploadFS.config.https
    });
  }

  /**
   * Deletes a file async
   * @param fileId
   * @param callback
   */
  delete(fileId, callback){
    throw new Error('delete is not implemented');
  };

  /**
   * Returns the file read stream
   * @param fileId
   * @param file
   */
  getReadStream(fileId, file){
    throw new Error('getReadStream is not implemented');
  };

  /**
   * Returns the file write stream
   * @param fileId
   * @param file
   */
  getWriteStream(fileId, file){
    throw new Error('getWriteStream is not implemented');
  };

  /**
   * Callback for copy errors
   * @param err
   * @param fileId
   * @param file
   * @return boolean
   */
  onCopyError(err, fileId, file){
    console.error(`ufs: cannot copy file "${ fileId }" (${ err.message })`);
  };

  /**
   * Called when a file has been uploaded
   * @param file
   */
  onFinishUpload = function (file) {};

  /**
   * Called when a file is read from the store
   * @param fileId
   * @param file
   * @param request
   * @param response
   * @return boolean
   */
  onRead(fileId, file, request, response){
    return true;
  }

  /**
   * Callback for read errors
   * @param err
   * @param fileId
   * @param file
   * @return boolean
   */
  onReadError(err, fileId, file) {
    console.error(`ufs: cannot read file "${ fileId }" (${ err.message })`);
  }

  /**
   * Callback for write errors
   * @param err
   * @param fileId
   * @param file
   * @return boolean
   */
  onWriteError(err, fileId, file) {
    console.error(`ufs: cannot write file "${ fileId }" (${ err.message })`);
  }
}


UploadFS.StorageAdapter = StorageAdapter;



    /**
     * Copies the file to a store
     * @param fileId
     * @param store
     * @param callback
     */
    copy(fileId, store, callback) {
      check(fileId, String);

      if (!(store instanceof StorageAdapter)) {
        throw new TypeError('store is not an UploadFS.StorageAdapter');
      }

      // Get original file
      var file = this.getCollection().findOne(fileId);
      if (!file) {
        throw new Meteor.Error(404, 'File not found');
      }

      // Prepare copy
      const copy = _.omit(file, '_id', 'url');
      copy.originalStore = this.getName();
      copy.originalId = fileId;

      // Create the copy
      const copyId = store.create(copy);

      // Get original stream
      const rs = this.getReadStream(fileId, file);

      // Catch errors to avoid app crashing
      rs.on('error', Meteor.bindEnvironment(function (error) {
        callback.call(this, error, null);
      }));

      // Copy file data
      store.write(rs, copyId, Meteor.bindEnvironment(function (err) {
        if (err) {
          store.getCollection().remove(copyId);
          this.onCopyError.call(this, err, fileId, file);
        }
        if (typeof callback === 'function') {
          callback.call(this, err, copyId, copy, store);
        }
      }));
    };

    /**
     * Transforms the file on reading
     * @param readStream
     * @param writeStream
     * @param fileId
     * @param file
     * @param request
     * @param headers
     */
    transformRead(readStream, writeStream, fileId, file, request, headers) {
      readStream.pipe(writeStream);
    };

    /**
     * Transforms the file on writing
     * @param readStream
     * @param writeStream
     * @param fileId
     * @param file
     */
    transformWrite(readStream, writeStream, fileId, file) {
      readStream.pipe(writeStream);
    };

    /**
     * Writes the file to the store
     * @param rs
     * @param fileId
     * @param callback
     */
    write(rs, fileId, callback) {
      const file = this.collection.findOne(fileId);
      const ws = this.getWriteStream(fileId, file);

      const errorHandler = Meteor.bindEnvironment(function (err) {
        this.collection.remove(fileId);
        this.onWriteError.call(this, err, fileId, file);
        callback.call(this, err);
      });

      ws.on('error', errorHandler);
      ws.on('finish', Meteor.bindEnvironment(function () {
        var size = 0;
        const readStream = this.getReadStream(fileId, file);

        readStream.on('error', Meteor.bindEnvironment(function (error) {
          callback.call(this, error, null);
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
          file.url = this.getFileURL(fileId);

          // Sets the file URL when file transfer is complete,
          // this way, the image will loads entirely.
          this.collection.update(fileId, {
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
          callback.call(this, null, file);

          // Execute callback
          if (typeof this.onFinishUpload == 'function') {
            this.onFinishUpload.call(this, file);
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
                this.copy(fileId, store);
              }
            }
          }
        }));
      }));

      // Execute transformation
      this.transformWrite(rs, ws, fileId, file);
    };
  }
