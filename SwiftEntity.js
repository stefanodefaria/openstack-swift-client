"use strict";

const requestp = require('request-promise');
const queryString = require('query-string');

class SwiftEntity {
    constructor(childName, urlSuffix, authenticator) {
        this.childName = childName;
        this.urlSuffix = urlSuffix ? `/${urlSuffix}` : '';
        this.authenticator = authenticator;
    }

    list(extra, query = '') {
        const querystring = ( query ) ? '?'+queryString.stringify(query) : query;
        return this.authenticator.authenticate().then(auth => requestp({
            uri: auth.url + this.urlSuffix + querystring,
            headers: this.headers(null, extra, auth.token),
            json: true
        }));
    }

    update(name, meta, extra) {
        return this.authenticator.authenticate().then(auth => requestp({
            method: 'POST',
            uri: `${auth.url + this.urlSuffix}/${name}`,
            headers: this.headers(meta, extra, auth.token)
        }));
    }

    meta(name) {
        return this.authenticator.authenticate().then(auth => requestp({
            method: 'HEAD',
            uri: `${auth.url + this.urlSuffix}/${name}`,
            headers: this.headers(null, null, auth.token),
            resolveWithFullResponse: true
        }).then(response => {
            const meta = {};
            const headers = response.headers;
            const regex = new RegExp(`^X-${this.childName}-Meta-(.*)$`, 'i');

            for (const k in headers) {
                const m = k.match(regex);

                if (m) {
                    meta[m[1]] = headers[k];
                }
            }

            return meta;
        }));
    }

    delete(name) {
        return this.authenticator.authenticate().then(auth => requestp({
            method: 'DELETE',
            uri: `${auth.url + this.urlSuffix}/${name}`,
            headers: this.headers(null, null, auth.token)
        }));
    }

    headers(meta, extra, token) {
        const headers = Object.assign({
            'accept': 'application/json',
            'x-auth-token': token
        }, extra);

        if (meta != null) {
            for (const k in meta) {
                if (meta.hasOwnProperty(k)) {
                    headers[`X-${this.childName}-Meta-${k}`] = meta[k];
                }
            }
        }

        return headers;
    }
}

module.exports = SwiftEntity;
