'use strict';


class Consumer {
    constructor( client ) {
        this.consumer = client;
        this.client   = new client.constructor( client.options );
    }

    subscribe( channels, strategy ) {
        for ( let index in channels ) {
            this.consumer.subscribe( channels[index] );
        }

        this.consumer.on( "subscribe", ( channel, count ) => {
            console.log( "client subscribed to " + channel + ", " + count + " total subscriptions" );
        } );

        this.consumer.on( "message", ( queue ) => {

            this.client.lpop( `queue:${queue}`, ( err, id ) => {
                if ( id !== null )
                    this.consume( queue, id, strategy )

            } );
        } );
    }

    consume( queue, id, strategy ) {
        let queue_job_key = `queue:${queue}:${id}`;
        let Consumer      = strategy[queue];
        if ( typeof Consumer !== 'function' )
            return console.error( `Consumer strategy [${queue}] is not defined!` );

        this.client.hgetall( queue_job_key, ( err, data ) => {
            if ( data === null )
                return console.error( `Queue task ${queue_job_key} not exist` );

            Consumer.call( this, data );
            return this.client.del( queue_job_key );
        } );

    }
}

module.exports = Consumer;
