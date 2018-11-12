"use strict";

const request = require('request');
const requestp = require('request-promise');
const queryString = require('query-string');
const SwiftEntity = require('./SwiftEntity');

class SwiftContainer extends SwiftEntity {
    constructor(containerName, authenticator) {
        super('Object', containerName, authenticator);
    }

    create({
        name = null,
        stream = null,
        meta = null,
        extra = null,
        query = null,
        content = null
    }) {
        if(!name || name == '')
            throw new Error('Name must not be empty');
        if(!stream)
            throw new Error('FileStream must not be empty');
        const querystring = query ? '?' + queryString.stringify(query) : '';

        return this.authenticator.authenticate().then(auth => new Promise((resolve, reject) => {
            const req = request({
                method: 'PUT',
                uri: `${auth.url + this.urlSuffix}/${name}` + querystring,
                headers: this.headers(meta, extra, auth.token),
                json: content
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

    delete({
        name = null,
        when = null,
        query = null
    }) {
        if(!name || name == '')
            throw new Error('Name must not be empty');
        const querystring = query ? '?' + queryString.stringify(query) : '';

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
                    uri: `${auth.url + this.urlSuffix}/${name}` + querystring,
                    headers: this.headers(null, h, auth.token)
                });
            });

        } else {
            return this.authenticator.authenticate().then(auth => requestp({
                method: 'DELETE',
                uri: `${auth.url + this.urlSuffix}/${name}` + querystring,
                headers: this.headers(null, null, auth.token)
            }));
        }
    }

    get({
        name = null,
        stream = null,
        query = null
    }) {
        if(!name || name == '')
            throw new Error('Name must not be empty');
        if(!stream)
            throw new Error('FileStream must not be empty');
        
        const querystring = query ? '?' + queryString.stringify(query) : '';
        return this.authenticator.authenticate().then(auth => new Promise((resolve, reject) => {
            request({
                method: 'GET',
                uri: `${auth.url + this.urlSuffix}/${name}` + querystring,
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

}

module.exports = SwiftContainer;
