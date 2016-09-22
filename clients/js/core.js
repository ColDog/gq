module.exports = function (request) {
    'use strict';

    class Graph {

        constructor(endpoint) {
            this.endpoint = endpoint || 'http://localhost:8231'
        }

        createNode(type, body, cb) {
            this.put('node:'+type, body, cb)
        }

        createEdge(type, source, target, body, cb) {
            this.put('edge:'+type+'.'+source+'.'+target, body, cb)
        }

        put(resourceID, body, cb) {
            request({
                method: 'PUT',
                url: this.endpoint+'/v1/resources/'+resourceID,
                json: body
            }, function (err, _, res) {
                cb && cb(err, res)
            });
        }

        del(resourceID, cb) {
            request({
                method: 'DELETE',
                url: this.endpoint+'/v1/resources/'+resourceID
            }, function (err, _, res) {
                cb && cb(err, res)
            });
        }

        get(resourceID, cb) {
            request({
                method: 'GET',
                url: this.endpoint+'/v1/resources/'+resourceID
            }, function (err, _, res) {
                cb && cb(err, res)
            });
        }

        traverse(t, cb) {
            request({
                method: 'POST',
                url: this.endpoint+'/v1/traverse',
                json: t
            }, function (err, _, res) {
                cb && cb(err, res)
            });
        }

    }

    class Traversal {

        constructor() {
            this.limit = 2000;
            this.next = null;
            this.filters = null;
            this.type = null;
            this.id = null;
        }

        is(type) {
            this.type = type;
            return this;
        }

        has(attr, val) {
            if (attr === 'id') {
                this.id = val
            } else {
                console.warn('attr ' + attr + ' is not supported.')
            }

            return this;
        }

        out() {
            let args = Array.prototype.slice.call(arguments);
            return this.rel(0, null, 1000, args);
        }

        in() {
            let args = Array.prototype.slice.call(arguments);
            return this.rel(1, null, 1000, args);
        }

        both() {
            let args = Array.prototype.slice.call(arguments);
            return this.rel(1, null, 1000, args);
        }

        outLimit(limit) {
            let args = Array.prototype.slice.call(arguments);
            return this.rel(0, null, args[0], args.slice(1));
        }

        inLimit(limit) {
            let args = Array.prototype.slice.call(arguments);
            return this.rel(1, null, args[0], args.slice(1));
        }

        bothLimit(limit) {
            let args = Array.prototype.slice.call(arguments);
            return this.rel(1, null, args[0], args.slice(1));
        }

        outFilter(filter) {
            let args = Array.prototype.slice.call(arguments);
            return this.rel(0, args[0], null, args.slice(1));
        }

        inFilter(filter) {
            let args = Array.prototype.slice.call(arguments);
            return this.rel(1, args[0], null, args.slice(1));
        }

        bothFilter(filter) {
            let args = Array.prototype.slice.call(arguments);
            return this.rel(1, args[0], null, args.slice(1));
        }

        groupBy(x) {
            this.filters.push('groupBy('+x+')');
            return this;
        }

        limitTo(x) {
            this.limit = x;
            return this;
        }

        rel(dir, filter, limit, relTypes) {
            this.next = {
                target: new Traversal(),
                direction: dir,
                limit: limit || 2000,
                filter: filter,
                types: relTypes
            };
            return this.next.target;
        }

    }
    
    return {
        Graph: Graph,
        Traversal: Traversal
    }
    
};
