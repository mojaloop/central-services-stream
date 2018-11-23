
# Message Protocol for Stream Processing

Based on LIME Protocol: http://limeprotocol.org/

## Envelope 

The JSON documents exchanged in a LIME conversation are called envelopes. The JSON data must be compliant with the RFC 4627 specification, using the UTF-8 encoding.

Every envelope may contains the following properties:

from - Identifier of the sender node of the envelope, in the name@domain/instance format. If a node receives an envelope without this value, it means that the envelope was originated by the remote party.
to - Identifier of the destination node of the envelope, in the same format of the sender. If a node receives an envelope without this value, it means that the envelope is addressed to itself.
pp - Acronym for per procurationem. Identifier of a delegate node (a node that received a permission to send on behalf of another), in the name@domain/instance format. Allows a node to send an envelope on behalf of another identity.
id - Identifier of the envelope, which can be any relevant value for the caller.
metadata - Allows the transport of generic information, in the "name": "value" format. This property is optional for all envelopes. Any value set in this property should not change the behavior of the server.

## Message

A message provides the transport of a content between nodes in a network.

#### JSON Schema:

```JSON
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "message",
    "type": "object",
    "properties": {
        "from": {
            "type": "string",
            "pattern": "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
        },
        "to": {
            "type": "string",
            "pattern": "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
        },
        "pp": {
            "type": "string",
            "pattern": "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
        },
        "id" : {
            "type": "string"
        },
        "metadata": {
            "type": "object"
        },
        "type" : {
            "type": "string",
            "pattern": "^[-\w]+/[-\w.]+(\+\w+)?$"
        },
        "content" : {
            "type": "object"
        },
    },
    "required": [ "type", "content" ],
    "additionalProperties": false
}
```

In a message, the following constraints should apply to the envelope properties:

id - Optional. The sender just need provide this information in order to receive notifications related to the message. If not provided, the message is processed in the fire-and-forget mode.
from - Optional for the sender, since the value can be determined by the server in the session. mandatory in the destination.
to - Mandatory for the sender, but is allowed to ommit the value of the instance (the server will route the envelope according to the routing rule chosen by the destination) and/or domain (the server will assume that the destination is in the same domain of the sender) properties. optional in the destination.
pp - Optional for the sender, when is considered the identity of the session. Is mandatory in the destination if the identity of the originator is different of the identity of the from property.
metadata - Optional. The sender should avoid to use this property to transport any kind of content-related information, but merely data relevant to the context of the communication. Consider to define a new content type if there's a need to include more content information into the message.
Besides the properties defined in the envelope, a message should contains:

type - MIME declaration of the content type of the message. This property is used by the destination to handle the content in the appropriate way. The MIME can be any discrete type (text, application, image, video or audio; composite types, like message or multipart, are not supported). For structured data, is recommended to use JSON subtypes (like application/json or application/vnd.specific+json), althought is possible to send other types, like XML, provided that the unsupported characters in content are escaped, accordinally to the JSON specification. The protocol defines some common types that should be reused. If you are using a custom content type, consider registering it with IANA in order to provide interoperability.
content - The representation of the content. JSON content data are represented unescaped. Binary data (images, video and audio) should be represented with the Base64 encoding. The remaining data types should be represented as an escaped value.
Examples

Fire-and-forget (no id) text message:

```JSON
{
    "from": "skyler@breakingbad.com/bedroom",
    "to": "ww@breakingbad.com",
    "type": "text/plain",
    "content": "Walter, are you in danger?"    
}
```

JSON text message with metadata and notification support, omitting the destination domain:

```JSON
{
    "id": "my-id",
    "from": "heisenberg@breakingbad.com/bedroom",
    "to": "skyler/bedroom",
    "type": "application/vnd.lime.threadedtext+json",
    "content": {
        "text": "I am the one who knocks!",
        "thread": 2
    },
    "metadata": {
        "senderIp": "192.168.0.1"
    }
}
```

Binary text message with notification support:

```JSON
{
    "id": "65603604-fe19-479c-c885-3195b196fe8e",   
    "to": "heisenberg@breakingbad.com/bedroom",
    "type": "image/png",
    "content": "iVBORw0KGgoAAAANSUhEUgAAACsAAAAqCAMAAAAd31JXAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAyJpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMC1jMDYwIDYxLjEzNDc3NywgMjAxMC8wMi8xMi0xNzozMjowMCAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvIiB4bWxuczp4bXBNTT0iaHR0cDovL25zLmFkb2JlLmNvbS94YXAvMS4wL21tLyIgeG1sbnM6c3RSZWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9zVHlwZS9SZXNvdXJjZVJlZiMiIHhtcDpDcmVhdG9yVG9vbD0iQWRvYmUgUGhvdG9zaG9wIENTNSBNYWNpbnRvc2giIHhtcE1NOkluc3RhbmNlSUQ9InhtcC5paWQ6MEYyQzgwNTUxQkY2MTFFMkE2NzBBRjFERkI1MEFCQjUiIHhtcE1NOkRvY3VtZW50SUQ9InhtcC5kaWQ6MEYyQzgwNTYxQkY2MTFFMkE2NzBBRjFERkI1MEFCQjUiPiA8eG1wTU06RGVyaXZlZEZyb20gc3RSZWY6aW5zdGFuY2VJRD0ieG1wLmlpZDowRjJDODA1MzFCRjYxMUUyQTY3MEFGMURGQjUwQUJCNSIgc3RSZWY6ZG9jdW1lbnRJRD0ieG1wLmRpZDowRjJDODA1NDFCRjYxMUUyQTY3MEFGMURGQjUwQUJCNSIvPiA8L3JkZjpEZXNjcmlwdGlvbj4gPC9yZGY6UkRGPiA8L3g6eG1wbWV0YT4gPD94cGFja2V0IGVuZD0iciI/PnkDBYEAAADAUExURcbhnr/flKbTaZPJSrPZfprMVKzWdMzlqcXinpTKS9jsvt7vyKDPX7ncieLxz9Los5vMVtTqt/j788bin6/Wec/lruby1dvswtXouJDIRfH459Lns+v13eDvy+r02+7246zUc6LRY6nTbev13t3txuXy05TJS73cj5vNV7HYe6LPYrjbh7bZhJ7OXL/fk7naiZfLUKrUb6XRZ9vtw+nz2uPw0LPXfsnipM3mq9jqvcDdk8zkqbzcjvH46P///43GP1/s3CQAAALWSURBVHjajJXpmqowDIYtlB1kVQE3wH0ddfalJ/d/V6fQAtUZz5n8kMfyJv2aJqHz5/fW+b4UdKkF/2UDf7cGOHmeB/nRv3UQ2XQHp2z/0RtQiz8fskOS+z+z6SbZrt6fCLOFRWbz4cth7f/A+kbRGRPiYmRBZYpFyGzw4eWTW9Y/rcbEkkpIZywgXdJH0zgzJtfsY7J6Ihpj+AM0hQbXyDiqYcamxv6JuHU8WfRRzHFmpAKbZ1QARwBL7KHW8eND3rJv3pCQ+hVIjDXr/9DtJGHNBsbD1NQaVkaVh9awznNhBJw9egMqQHKlWkR9stqZ9JYhZ2lYiSVJblgFQWvWbF8GpuzjMuYpUFjo8kdAJcsksedX7LHoN1qxzji5WVEdLIM2izYVm+/bHICtUeUyqJUaGevIrtSRjlGx8EmEHWVs06SVkm0OljLIEBjbE1mqw0KOAoqO2yWFvEOXsl3KjhAyTVNDSDN1ZNKSxKoqusOi17AES6XxF5S14NrMTqWhZB1x3RkRotHjS2KGv7jeLyKsSrriuIsyDQrGTn04sufsS8sqLq1IVZHrw9oqKtOnkmhdseuiX2+numUVUEW4rQbV1fRRv2B3sfPmrKZknR3eub5jqpbMvTdWD8lwVO3HS8cuPbAsZoHEyYTXWTSjWzp1RquQsnPFrta8Jnen2MWmDWL5gi4ENp8vR86m8DBtOhNs5qTowg33lmndb+ftgDQtg4SWZ+b2o2PTm11YzRZX3VZGq73x9PUUtD2/oY3MmTazXLFE4kMozIfAyOYL8WTVJbB+Hs0v+dWM6kI0Rs1FcBvJ9IRkfjOj6OyDbDijtSAJ1WWWg7V3uZ199PaMZTTsC+OH7j4dv0bLPP0+q4PQWGad8dRCNLSN9cV08LEFCO98L/w1bF+G8z6150G8ouR5cv871N1A4l2Kojh4CZzD9N/frMAPz7swDP3Jb75vd+2vAAMAIkNXL7uoiZEAAAAASUVORK5CYII="
}
```

## Notification

A notification transports information about events associated to a message sent in a session. Can be originated by a server or by the message destination node.

#### JSON Schema:

```JSON
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "notification",
    "type": "object",
    "properties": {
        "from": {
            "type": "string",
            "pattern": "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
        },
        "to": {
            "type": "string",
            "pattern": "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
        },
        "pp": {
            "type": "string",
            "pattern": "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
        },
        "id" : {
            "type": "string"
        },
        "metadata": {
            "type": "object"
        },
        "event" : {
            "enum": [ 
                "accepted",
                "validated",
                "authorized",
                "dispatched",
                "received",
                "consumed",
                "failed"
            ]
        },
        "reason" : {
            "type": "object",
            "properties": {
                "code" : {
                    "type": "integer"
                },
                "description": {
                    "type": "string"
                }
            },
            "required": [ "code" ]
        }
    },
    "required": [ "id", "event" ],
    "additionalProperties": false
}
```

In a notification, the following constraints should apply to the envelope properties:

id - Mandatory. The id references the related message.
from - Optional. If the value is not present in the destination, it means that the notification was generated by the connected node (server).
to - Mandatory for the sender and optional in the destination. The sender can ommit the value of the domain.
pp - Optional for the sender, when is considered the identity of the session. Is mandatory in the destination if the identity of the originator is different of the identity of the from property.
metadata - Optional. Avoid to use this property to transport any event-related data.
Besides the envelope properties, a notification may contains:

event - Name of the event. This property is mandatory. The possible values are:
accepted - The message was received and accepted by the server.
validated - The message format was validated by the server.
authorized - The dispatch of the message was authorized by the server.
dispatched - The message was dispatched to the destination by the server.
received - The destination has received the message. This event is generated by the destination node.
consumed - The destination has consumed (read or processed) the message. This event is generated by the destination node.
failed - A problem occurred during the processing of the message. In this case, the reason property of the notification should be present.
reason - In the case of a failed event, this property brings more details about the problem. It contains the following properties:
code - Code number of the reason. There are some protocol pre-defined codes, but may exists specific codes for each implementation of the protocol.
description - Description message of the problem. This property can be omitted.
Examples

Received notification sent by the destination node:

```JSON
{
    "id": "48600604-ce09-479c-b985-1195b196fe8e",
    "from": "skyler@breakingbad.com/bedroom",
    "to": "heisenberg@breakingbad.com/bedroom",
    "event": "received"
}
```

Failure notification sent by the server:

```JSON
{
    "id": "9d0c4fea-75c7-432a-a164-c1a219bc17a8",
    "to": "skyler@breakingbad.com/bedroom",
    "event": "failed",
    "reason": {
        "code": 42,
        "description": "The message destination was not found"
    }
}
```

## Command

Allows the manipulation of node resources, like server session parameters or information related to the network nodes.

#### JSON Schema:

```JSON
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "command",
    "type": "object",
    "properties": {
        "from": {
            "type": "string",
            "pattern": "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
        },
        "to": {
            "type": "string",
            "pattern": "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
        },
        "pp": {
            "type": "string",
            "pattern": "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})$"
        },
        "id" : {
            "type": "string"
        },
        "metadata": {
            "type": "object"
        },
        "method" : {
            "enum": [ 
                "get",
                "set",
                "merge",
                "delete",
                "subscribe",
                "unsubscribe",
                "observe"
            ]
        },
        "uri" : {
            "title": "limeUri",
            "type": "string",
            "pattern": "^((lime://)(\w\.?-?)+@@?(\w\.?-?@@?)+)?(/(\w\.?-?@@?)+)+(\?{1}((\w+=\w+)&?)+)?$"
        },
        "type" : {
            "type": "string",
            "pattern": "^[-\w]+/((json)|([-\w.]+(\+json)))$"
        },
        "resource" : {
            "type": "object"
        },
        "status": {
            "enum": [
                "success",
                "failure"
            ]
        },
        "reason" : {
            "type": "object",
            "properties": {
                "code" : {
                    "type": "integer"
                },
                "description": {
                    "type": "string"
                }
            },
            "required": [ "code" ]
        }
    },
    "required": [ "id", "method" ],
    "additionalProperties": false
}
```

In a command, the following constraints should apply to the envelope properties:

id - Mandatory, except for the observe method. Must be provided during the request and the same value should be in the related response command.
from - Optional. If the value is not present in the destination, it means that the notification was generated by the connected node (server).
to - Optional. If the value is left empty in the request, the command will be processed by the connected node (server).
pp - Optional. This property is useful to get resources in the server owned by different identities, like presence and account information. The value should be defined if the resource of an observe method command is owned by a different identity than the destination.
metadata - Optional. Avoid to use this property to transport any command-related data, like extra parameters.
Besides the envelope properties, a command may contains:

method - Method for the manipulation of the resource. This property is mandatory. The possible values are:
get - Gets an existing value of the resource.
set - Sets or updates a for the resource.
merge - Merges the resource document with an existing one. If the resource doesn't exists, it is created.
delete - Deletes a value of the resource or the resource itself.
subscribe - Subscribes to the resource, allowing the originator to be notified when the value of the resource changes in the destination.
unsubscribe - Unsubscribes to the resource, signaling to the destination that the originator do not want to receive further notifications about the resource.
observe - Notify the destination about a change in the resource value of the sender. Commands with this method are one-way and the destination should not send a response for it. Because of that, these commands may not have an id.
uri - The uniform resource identifier (RFC 3986). This property is mandatory in command requests and can be ommited in responses. It must be present in observe commands. The supported scheme is lime, and the authority must be always the identity of the resource owner. In command requests, this identity is always the same of the identity of from envelope property. Since the base URI is always the same for a specific identity (lime://user@domain), the URI can be represented as a fragment, like /groups, instead of lime://user@domain/groups. The URI may als contains a query string for filtering.
type - MIME type of the resource. Like the messages, the protocol defines some common resource types, but may exists specific resources to the implementation. This property should be present always when the value of resource property is defined.
resource - JSON representation of the resource. It must be present in commands with the methods set and observe and may be present in the method delete. In a command response, must be present in successfully processed command with the method get.
In addition of the above, the command response may contains the following properties:

status - Result status of the command processing. Mandatory in response commands. The valid values are:
success - The command was processed successfully. In cases of command of method get, the property resource of the response should have a value.
failure - A problem occurred while processing the command. In this case, the property reason of the response should have a value.
reason - If the command was not successfully processed, this property should provide more details about the problem. It contains the following properties:
code - Code number of the reason. There are some protocol pre-defined codes, but may exists specific codes for each implementation of the protocol.
description - Description message of the problem. This property can be omitted.
Examples

Setting the presence:

C:

```JSON
{
    "id": "9cbe5fe1-b6b2-4afe-ab12-0675aa139f36",
    "from": "jesse@breakingbad.com/home", 
    "method": "set",
    "uri": "/presence",
    "type": "application/vnd.lime.presence+json",
    "resource": {
        "status": "available",
        "message": "Yo 148, 3-to-the-3-to-the-6-to-the-9. Representin' the ABQ. What up, biatch?"
    }
}
```

S:

```JSON
{
    "id": "9cbe5fe1-b6b2-4afe-ab12-0675aa139f36",
    "from": "server@breakingbad.com",    
    "method": "set",
    "status": "success"    
}
```

Getting the roster with a filter:

C:

```JSON
{
    "id": "b784a7d2-59d7-45de-a1eb-8c50a0f5edb8",
    "from": "jesse@breakingbad.com/home",    
    "method": "get",
    "uri": "lime://jesse@breakingbad.com/contacts?sharePresence=true&take=3"    
}
```

S:

```JSON
{
    "id": "b784a7d2-59d7-45de-a1eb-8c50a0f5edb8",
    "from": "server@breakingbad.com",    
    "method": "get",
    "status": "success",
    "type": "application/vnd.lime.collection+json",
    "resource": {
        "total": "4",
        "itemType": "application/vnd.lime.contact+json",
        "items": [
            { 
                "identity": "skinnypete@breakingbad.com"
            },
            { 
                "identity": "badger@breakingbad.com"
            },
            { 
                "identity": "combo@breakingbad.com"
            }
        ]
    }
}
```

Trying without success to get another user account information:

C:

```JSON
{
    "id": "5c95e87f-7f0c-4cc3-b55b-99056d9f288d",
    "from": "gusfring@breakingbad.com",
    "pp": "saul@breakingbad.com",
    "method": "get",
    "uri": "lime://gusfring@breakingbad.com/account"
}
```

S:

```JSON
{
    "id": "5c95e87f-7f0c-4cc3-b55b-99056d9f288d",
    "from": "server@breakingbad.com",
    "method": "get",
    "status": "failure",
    "reason": {
        "code": 56,
        "description": "The request account is not sharing information with current session identity"
    }
}
```
