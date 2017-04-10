
const moment       = require('moment')
const xmlstream    = require('xml-stream')
const pd           = require('pretty-data').pd
const zmq          = require('zmq')
const zlib         = require('zlib')
const jsonStream   = require('jsonstream2')
const { Readable } = require('stream')
const { Transform } = require('stream')

const pub = zmq.socket('pub')
const sub = zmq.socket('sub')

function now() {
	const format = 'YYYY-MM-DD H:mm:ss'
	return moment(new Date).format(format)
}

sub.connect('tcp://pubsub.besteffort.ndovloket.nl:7658');
sub.monitor()

// // Register to monitoring events
// sub.on('connect', function(fd, ep) {console.log('connect, endpoint:', ep);});
// sub.on('connect_delay', function(fd, ep) {console.log('connect_delay, endpoint:', ep);});
// sub.on('connect_retry', function(fd, ep) {console.log('connect_retry, endpoint:', ep);});
// sub.on('listen', function(fd, ep) {console.log('listen, endpoint:', ep);});
// sub.on('bind_error', function(fd, ep) {console.log('bind_error, endpoint:', ep);});
// sub.on('accept', function(fd, ep) {console.log('accept, endpoint:', ep);});
// sub.on('accept_error', function(fd, ep) {console.log('accept_error, endpoint:', ep);});
// sub.on('close_error', function(fd, ep) {console.log('close_error, endpoint:', ep);});
// sub.on('disconnect', function(fd, ep) {console.log('disconnect, endpoint:', ep);});
sub.subscribe('/QBUZZ/KV6posinfo');

console.log(now(), 'started listening');

class Prettify extends Transform {
	constructor(options) {
		super(options)
	}
	_transform(obj, enc, cb) {
		cb(null, pd.xml(obj.toString()))
	}
}

class Filter extends Transform {
	constructor(options, filter) {
		super(options)
	}
	_transform(obj, enc, cb) {
		cb(null, pd.xml(obj.toString()))
	}
}

class Wrapper extends Readable {
	constructor({options, subje}) {
		super(options)
		this._source = subje
		this._source.on('message', (topic, message) => {
			console.log(now(), 'received a message related to:', topic.toString(), 'size:', message.length)
			if(!this.push(message)) {
				console.log('Buffer full')
			}
		});
		this._source.on('close', (fd, ep) => {
			console.log('close, endpoint:', ep)
			this.push(null)
		});

	}

	_read(size) {
	}

}

const wrapper = new Wrapper({ subje: sub })
const unzip = zlib.createGunzip()
const pretty  = new Prettify()

wrapper.pipe(unzip).pipe(pretty).pipe(process.stdout)
