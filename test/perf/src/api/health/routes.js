const Handler = require('./handler')
const tags = ['api', 'metadata']

module.exports = [
  {
    method: 'GET',
    path: '/health',
    handler: Handler.getHealth,
    options: {
      tags: tags,
      description: 'Status of adapter',
      id: 'health'
    }
  }
]
