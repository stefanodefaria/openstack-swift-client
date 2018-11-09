"use strict";

const request = require('request');
const requestp = require('request-promise');
const SwiftEntity = require('./SwiftEntity');

class SwiftContainer extends SwiftEntity {
    constructor(containerName, authenticator) {
        super('Object', containerName, authenticator);
    }

    create(name, stream, meta, extra) {

        return this.authenticator.authenticate().then(auth => new Promise((resolve, reject) => {
            const req = request({
                method: 'PUT',
                uri: `${auth.url + this.urlSuffix}/${name}`,
                headers: this.headers(meta, extra, auth.token)
            }).on('error', err => {
                reject(err);
            }).on('response', response => {
                if (response.statusCode === 201) {
                    resolve({
                        etag: response.headers.etag
                    });
                } else {
                    reject(new Error(`HTTP ${response.statusCode}`));
                }
            });

            stream.pipe(req);
        }));
    }

    delete(name, when) {
        if (when) {
            const h = {};

            if (when instanceof Date) {
                h['X-Delete-At'] = +when / 1000;
            } else if (typeof when === 'number' || when instanceof Number) {
                h['X-Delete-After'] = when;
            } else {
                throw new Error('expected when to be a number of seconds or a date');
            }

            return this.authenticator.authenticate().then(auth => {
                return requestp({
                    method: 'POST',
                    uri: `${auth.url + this.urlSuffix}/${name}`,
                    headers: this.headers(null, h, auth.token)
                });
            });

        } else {
            return SwiftEntity.prototype.delete.call(this, name);
        }
    }

    get(name, stream) {
        return this.authenticator.authenticate().then(auth => new Promise((resolve, reject) => {
            request({
                method: 'GET',
                uri: `${auth.url + this.urlSuffix}/${name}`,
                headers: {
                    'x-auth-token': auth.token
                }
            }).on('error', err => {
                reject(err);
            }).on('end', () => {
                resolve();
            }).pipe(stream);
        }));
    }
    
    createStaticLargeObject(name, manifestList, meta, extra) {
        // for manifestList
        extra = Object.assign({
            'content-type': 'application/json'
        }, extra);

        return this.authenticator.authenticate().then(auth => new Promise((resolve, reject) => {
            request({
                uri: `${auth.url + this.urlSuffix}/${name}?multipart-manifest=put`,
                method: 'PUT',
                headers: this.headers(meta, extra, auth.token),
                json: manifestList
            }).on('error', err => {
                reject(err);
            }).on('response', response => {
                if (response.statusCode === 201) {
                    resolve(response);
                } else {
                    reject(new Error(`HTTP ${response.statusCode}`));
                }
            });
        }));
    }
}

module.exports = SwiftContainer;
