const optionsSchema = new SimpleSchema({
    contentTypes: {
        type: Array,
        optional:true
    },
    extensions: {
        type: Array,
        optional:true
    },
    minSize: {
        type: Number,
    },
    maxSize: {
        type: Number,
    },
    onCheck: {
        type: Function,
        optional: true
    },
});

/**
 * File filter
 * @param options
 * @constructor
 */
class Filter {
    /**
     * Checks the file
     * @param file
     */
    check(file) {}

    /**
     * Checks if the file matches filter
     * @param file
     * @return {boolean}
     */
    isValid(file) {
        try {
            this.check(file);
            return true;
        } catch (e) {
            return false;
        }
    }
}

class CombinedFilter extends Filter{
    constructor(filters){
        this._filters = filters;
    }

    get filters(){
        return this._filters;
    }

    check(file){
        this._filters.forEach(filter => filter.check(file));
    }
}

class ContentTypeFilter extends Filter{
    constructor(contentTypes){
        this._contentTypes = contentTypes;
    }
    get contentTypes(){
        return this._contentTypes;
    }

    checkContentType(type, list) {
        if (list.indexOf(type) > -1) {
            return true;
        } else {
            const wildCardGlob = '/*';
            const wildcards = list.filter(item => item.indexOf(wildCardGlob) > 0);

            if (_.contains(wildcards, type.replace(/(\/.*)$/, wildCardGlob))) {
                return true;
            }
        }
        return false;
    }

    check(file){
        super.check(file);

        // Check content type
        if (this.contentTypes && !this.checkContentType(file.type, this.contentTypes)) {
            throw new Meteor.Error('invalid-file-type', 'File type is not accepted');
        }
    }
}

class ExtensionsFilter extends Filter{
    constructor(extensions){
        this._extensions = extensions;
    }
    get extensions(){
        return this._extensions;
    }

    check(file){
        super.check(file);

        // Check content type
        if (this.extensions.indexOf(file.extension) === -1) {
            throw new Meteor.Error('invalid-file-extension', 'File extension is not accepted');
        }
    }
}

class MinSizeFilter extends Filter{
    constructor(minSize){
        if (typeof minSize !== 'number') {
            throw new TypeError('minSize is not a number');
        }

        this._minSize = minSize;
    }

    get minSize(){
        return this._minSize;
    }

    check(file){
        super.check(file);

        // Check content type
        if (file.size <= 0 || file.size < this.minSize) {
            throw new Meteor.Error('file-too-small', `File is too small (min = ${ this.minSize })`);
        }
    }
}

class MaxSizeFilter extends Filter{
    constructor(maxSize){
        if (typeof maxSize !== 'number') {
            throw new TypeError('maxSize is not a number');
        }

        this._maxSize = maxSize;
    }

    get maxSize(){
        return this._maxSize;
    }

    check(file){
        super.check(file);

        // Check content type
        if (file.size > this.maxSize) {
            throw new Meteor.Error('file-too-large', `File is too large (max = ${ this.maxSize })`);
        }
    }
}

class SizeFilter extends CombinedFilter{
    constructor(minSize,maxSize){
        super(
          new MinSizeFilter(minSize),
          new MaxSizeFilter(maxSize)
        );
    }
}

class AdhocFilter extends Filter{
    constructor(onCheck){
        if (typeof onCheck !== 'function') {
            throw new TypeError('onCheck is not a function');
        }

        this._onCheck = onCheck;
    }
    get onCheck(){
        return this._onCheck;
    }

    check(file){
        super.check(file);

        // Apply custom check
        if (!this.onCheck.call(this, file)) {
            throw new Meteor.Error('invalid-file', 'File does not match filter');
        }
    }
}