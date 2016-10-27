'use strict';

const uuid = require( 'uuid' );

class Publisher {
    constructor( client, queue ) {
        this.client = client;
        this.queue  = queue || 'common';
    }

    publish( data ) {
        let id = uuid.v4();

        this.client.rpush( `queue:${this.queue}`, id, () => {
            this.client.hmset( `queue:${this.queue}:${id}`, data, () => {
                this.client.publish( this.queue, id );
            } );
        } );
    }
}

module.exports = Publisher;

