const getRawBody = require('raw-body')
const bufferToDataUri = require('../protocol/buffer').toDataUri

const requestRawPayloadTransform = (request, payloadBuffer = request.payload) => {
  if ((request && request.headers && request.headers['content-type']) && (payloadBuffer && payloadBuffer instanceof Buffer)) {
    try {
      return Object.assign(request, {
        payload: payloadBuffer.toString(),
        dataUri: bufferToDataUri(request.headers['content-type'], payloadBuffer),
        rawPayload: payloadBuffer
      })
    } catch (e) {
      throw e
    }
  } else {
    throw new Error('wrong input')
  }
}

module.exports.plugin = {
  name: 'rawPayloadTransform',
  register: (server, options) => {
    if (server.settings.routes.payload.output === 'stream' && server.settings.routes.payload.parse) {
      server.ext([{
        type: 'onPostAuth',
        method: async (request, h) => {
          return getRawBody(request.payload)
            .then(rawBuffer => {
              request = requestRawPayloadTransform(request, rawBuffer)
              request.reformat()
              return h.continue
            }).catch(e => {
              return h.continue
            })
        }
      }, {
        type: 'onPreHandler',
        method: async (request, h) => {
          request.payload = request.rawPayload
          return h.continue
        }
      }])
    }
  }
}
