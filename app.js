const _            = require('underscore')
const moment       = require('moment')
const parseString  = require('xml2js').parseString
const pd           = require('pretty-data').pd
const util         = require('util')
const zmq          = require('zmq')
const zlib         = require('zlib')
const jsonStream   = require('jsonstream2')
const { Readable } = require('stream')
const { Writable } = require('stream')
const { Transform } = require('stream')

const pub = zmq.socket('pub')
const sub = zmq.socket('sub')

function log(thing) {
	console.log(util.inspect(thing, false, null))
}

function now() {
	const format = 'YYYY-MM-DD H:mm:ss'
	return moment(new Date).format(format)
}

sub.connect('tcp://pubsub.besteffort.ndovloket.nl:7658')
sub.monitor()

// // Register to monitoring events
// sub.on('connect', function(fd, ep) {console.log('connect, endpoint:', ep)})
// sub.on('connect_delay', function(fd, ep) {console.log('connect_delay, endpoint:', ep)})
// sub.on('connect_retry', function(fd, ep) {console.log('connect_retry, endpoint:', ep)})
// sub.on('listen', function(fd, ep) {console.log('listen, endpoint:', ep)})
// sub.on('bind_error', function(fd, ep) {console.log('bind_error, endpoint:', ep)})
// sub.on('accept', function(fd, ep) {console.log('accept, endpoint:', ep)})
// sub.on('accept_error', function(fd, ep) {console.log('accept_error, endpoint:', ep)})
// sub.on('close_error', function(fd, ep) {console.log('close_error, endpoint:', ep)})
// sub.on('disconnect', function(fd, ep) {console.log('disconnect, endpoint:', ep)})
sub.subscribe('/QBUZZ/KV6posinfo')

console.log(now(), 'started listening')

class ObjectTransform extends Transform {
	constructor(options) {
		options = options || {}
		options.readableObjectMode = true
		options.writableObjectMode = true
		super(options)
	}
	_transform(obj, enc, cb) {
	}

}

class Prettify extends ObjectTransform {
	constructor(options) {
		super(options)
	}
	_transform(obj, enc, cb) {
		cb(null, pd.json(obj))
	}
}

class ToString extends ObjectTransform {
	constructor(options) {
		super(options)
		this.buf = null
	}
	_transform(obj, enc, cb) {
		// console.log(Buffer.byteLength(obj, 'utf-8'))
		cb(null, obj.toString() )
	}
}

class Printer extends Writable {
	constructor(options) {
		options = options || {}
		options.objectMode = true
		super(options)
		this.init      = []
		this.arrival   = []
		this.onstop    = []
		this.departure = []
		this.onroute   = []
		this.offroute  = []
		this.end       = []
		setInterval(() => {
			function filter(array) {
				return _.chain(array)
					.flatten()
					.filter((i) => {
						const num = parseInt(i.vehiclenumber)
						return num >= 4300 && num <= 4303
					})
				.value()

			}

			// Transmitted when a vehicle is connected to a journey
			const init = filter(this.init)
			if(init.length > 0) {
				console.log('####')
				console.log('INIT')
				console.log('####')
				console.log(now())
				console.log(init)
			}

			// Transmitted wVhen a vehicle arrived at a stop
			const arrival = filter(this.arrival)
			if(arrival.length > 0) {
				console.log('#######')
				console.log('ARRIVAL')
				console.log('#######')
				console.log(now())
				console.log(arrival)
			}

			const onstop = filter(this.onstop)
			if(onstop.length > 0) {
				console.log('######')
				console.log('ONSTOP')
				console.log('######')
				console.log(now())
				console.log(onstop)
			}

			const departure = filter(this.departure)
			if(departure.length > 0) {
				console.log('#########')
				console.log('DEPARTURE')
				console.log('#########')
				console.log(now())
				console.log(departure)
			}

			const onroute = filter(this.onroute)
			if(onroute.length > 0) {
				console.log('#######')
				console.log('ONROUTE')
				console.log('#######')
				console.log(now())
				console.log(onroute)
			}

			const offroute = filter(this.offroute)
			if(offroute.length > 0) {
				console.log('#######')
				console.log('ONROUTE')
				console.log('#######')
				console.log(now())
				console.log(offroute)
			}

			const end = filter(this.end)
			if(end.length > 0) {
				console.log('###')
				console.log('END')
				console.log('###')
				console.log(now())
				console.log(end)
			}

			this.init      = []
			this.arrival   = []
			this.onstop    = []
			this.departure = []
			this.onroute   = []
			this.offroute  = []
			this.end       = []
		}, 1000)
	}
	_write(obj, enc, cb) {
		if(obj.init) {
			this.init.push((obj.init))
		}
		if(obj.arrival) {
			this.arrival.push((obj.arrival))
		}
		if(obj.onstop) {
			this.onstop.push((obj.onstop))
		}
		if(obj.departure) {
			this.departure.push((obj.departure))
		}
		if(obj.onroute) {
			this.onroute.push((obj.onroute))
		}
		if(obj.offroute) {
			this.offroute.push((obj.offroute))
		}
		if(obj.end) {
			this.end.push((obj.end))
		}
		cb(null, obj)
	}
}

class XMLtoJS extends ObjectTransform {
	constructor(options) {
		super(options)
	}
	_transform(xmlstring, enc, cb) {
		// console.log('xmlstring', xmlstring)
		parseString(xmlstring, {trim: true, normalizeTags: true, explicitArray: false}, (err, result) => {
			if(result) return cb(null, result)
			cb(null, null)
		})
	}
}

class Filter extends ObjectTransform {
	constructor(options, filter) {
		super(options)
	}
	_transform(obj, enc, cb) {
		if(!obj.vv_tm_push) return cb(null, {data: null})
		if(!obj.vv_tm_push.kv6posinfo) return cb(null, {data: null})
		const stuff = obj.vv_tm_push.kv6posinfo
		// log(stuff)
		cb(null, stuff)
	}
}

class Wrapper extends Readable {
	constructor({options, subje}) {
		options = options || {}
		options.highWaterMark = 32 * 1024
		super(options)
		this._source = subje
		this._source.on('message', (topic, message) => {
			// console.log()
			// console.log(now())
			// console.log('received a message related to:', topic.toString(), 'size:', Buffer.byteLength(message, 'utf-8'))
			// console.log()
			zlib.gunzip(message, (err, res) => {
				if(!this.push(res)) {
					console.log("buffer full, closing")
					this._source.close()
				}
			})
		})
		this._source.on('close', (fd, ep) => {
			console.log('close, endpoint:', ep)
			this.push(null)
		})
	}

	_read(size) {
	}

}

const wrapper   = new Wrapper({ subje: sub })
const toString  = new ToString()
const pretty    = new Prettify()
const xmltojs   = new XMLtoJS()
const filter    = new Filter()
const printer   = new Printer()
const stringify = jsonStream.stringify(false)

wrapper
	.pipe(toString)
	.pipe(xmltojs)
	.pipe(filter)
	.pipe(printer)
