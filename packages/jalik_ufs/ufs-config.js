/**
 * UploadFS configuration
 * @param options
 * @constructor
 */
class Config{
    constructor (options = {}) {
        // Set default options
        options = Object.assign({
            https: false,
            simulateReadDelay: 0,
            simulateUploadSpeed: 0,
            simulateWriteDelay: 0,
            storesPath: 'ufs',
            tmpDir: '/tmp/ufs'
        }, options);

        // Check options
        if (typeof options.https !== 'boolean') {
            throw new TypeError('https is not a function');
        }
        if (typeof options.simulateReadDelay !== 'number') {
            throw new Meteor.Error('simulateReadDelay is not a number');
        }
        if (typeof options.simulateUploadSpeed !== 'number') {
            throw new Meteor.Error('simulateUploadSpeed is not a number');
        }
        if (typeof options.simulateWriteDelay !== 'number') {
            throw new Meteor.Error('simulateWriteDelay is not a number');
        }
        if (typeof options.storesPath !== 'string') {
            throw new Meteor.Error('storesPath is not a string');
        }
        if (typeof options.tmpDir !== 'string') {
            throw new Meteor.Error('tmpDir is not a string');
        }

        // Public attributes
        this.https = options.https;
        this.simulateReadDelay = parseInt(options.simulateReadDelay);
        this.simulateUploadSpeed = parseInt(options.simulateUploadSpeed);
        this.simulateWriteDelay = parseInt(options.simulateWriteDelay);
        this.storesPath = options.storesPath;
        this.tmpDir = options.tmpDir;
    }
} 

/**
 * Simulation read delay in milliseconds
 * @type {number}
 */
Config.prototype.simulateReadDelay = 0;

/**
 * Simulation upload speed in milliseconds
 * @type {number}
 */
Config.prototype.simulateUploadSpeed = 0;

/**
 * Simulation write delay in milliseconds
 * @type {number}
 */
Config.prototype.simulateWriteDelay = 0;

/**
 * URL path to stores
 * @type {string}
 */
Config.prototype.storesPath = null;

/**
 * Local temporary directory for uploading files
 * @type {string}
 */
Config.prototype.tmpDir = null;

/**
 * Global configuration
 * @type {Config}
 */
const config = new Config();

UploadFS.config = config;