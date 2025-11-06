import { randomUUID } from 'node:crypto'
import net from 'node:net'
import { TLSSocket } from 'node:tls'

import { WSConnector } from '../connections/ws.js'
import { BaseEndpoint } from './base.js'

const MAX_BRIGHTNESS = 10

function parseContentItem(data) {
    return {
        id: data.content_id,
        date: data.image_date ? parseDate(data.image_date) : undefined,
        categoryId: data.category_id,
        slideshow: data.slideshow === 'true',
        matte: parseMatte(data.matte_id),
        portraitMatte: parseMatte(data.portrait_matte_id),
        width: data.width,
        height: data.height,
    }
}

function parseDate(dateString) {
    const [date, time] = dateString.split(' ')
    return new Date(Date.parse(`${date.replaceAll(':', '-')} ${time}`))
}

function parseMatte(matteId) {
    if (matteId === 'none') return null
    const [type, color] = matteId.split('_')
    return { type, color }
}

// Possible event names that can be emitted:
// - art_mode_changed: Toggle art mode on/off
// - get_artmode_settings: Returning current art mode settings
// - go_to_standby: TV going to standby
// - image_selected: New image has been selected
// - recently_set_updated: Recent items list has been updated
// - set_brightness: Brightness was changed
// - wakeup
export class ArtModeEndpoint extends BaseEndpoint {
    constructor(args) {
        super()
        this.connection = new WSConnector({
            port: 8002,
            ...args,
            name: `${args.name}Art`,
            endpoint: 'com.samsung.art-app',
        })
    }
    async deleteArt(ids) {
        if (!Array.isArray(ids)) ids = [ids]
        try {
            const { content_id_list: contentList } = await this.request({
                action: 'delete_image_list',
                // eslint-disable-next-line camelcase
                content_id_list: ids.map(id => ({ content_id: id }))
            })
            return JSON.parse(contentList).map(item => ({ id: item.content_id }))
        } catch (e) {
            throw new Error(`Item does not exist (${e})`)
        }
    }
    async getAPIVersion() {
        const { version } = await this.request({ action: 'api_version' })
        return version
    }
    getArtModeInfo() {
        return this.request({ action: 'get_device_info' })
    }
    async getAvailableArt() {
        const { content_list: contentList } = await this.request({ action: 'get_content_list', category_id: 'MY-C0002' })
        return JSON.parse(contentList).map(parseContentItem)
    }
    async getBrightness() {
        const { data } = await this.request({ action: 'get_artmode_settings' })
        const setting = JSON.parse(data).find(({ item }) => item === 'brightness')
        return parseInt(setting.value)
    }
    async getCurrentArt() {
        const result = await this.request({ action: 'get_current_artwork' })
        return parseContentItem(result)
    }
    async getMatteColors() {
        const { matte_color_list: colors } = await this.request({ action: 'get_matte_list' })
        return JSON.parse(colors).map(c => c.color)
    }
    async getMatteTypes() {
        const { matte_type_list: types } = await this.request({ action: 'get_matte_list' })
        return JSON.parse(types).map(t => t.matte_type)
    }
    async getThumbnail(contentId) {
        const id = randomUUID()
        
        console.log(`[getThumbnail] Starting for contentId: ${contentId}, requestId: ${id}`)
        
        // Manually send WebSocket message (not using request() because it waits for wrong event)
        const message = {
            method: 'ms.channel.emit',
            params: {
                event: 'art_app_request',
                to: 'host',
                data: JSON.stringify({
                    request_id: id,
                    request: 'get_thumbnail',
                    content_id: contentId,
                    conn_info: {
                        d2d_mode: 'socket',
                        connection_id: Math.floor(Math.random() * 4 * 1024 ** 3),
                        id,
                    },
                    id,
                }),
            }
        }
        
        console.log(`[getThumbnail] Sending WebSocket message for request_id: ${id}`)
        this.connection.socket.send(JSON.stringify(message))
        
        // Implement event loop to wait for d2d_service_message event with matching request_id
        let response
        let connectionInfo
        const timeoutMs = 30000  // 30 seconds timeout
        
        console.log(`[getThumbnail] Waiting for d2d_service_message with matching request_id ${id}, timeout ${timeoutMs}ms`)
        
        // Wrap event loop in a timeout promise
        const eventLoopPromise = new Promise(async (resolve, reject) => {
            const timeoutId = setTimeout(() => {
                console.log(`[getThumbnail] TIMEOUT after ${timeoutMs}ms waiting for d2d_service_message with request_id ${id}`)
                reject(new Error(`Timeout waiting for d2d_service_message event after ${timeoutMs}ms`))
            }, timeoutMs)
            
            try {
                // Keep reading WebSocket messages until we get d2d_service_message with our request_id
                let messageCount = 0
                while (true) {
                    const wsMessage = await new Promise((msgResolve, msgReject) => {
                        let resolved = false
                        
                        const onMessage = (data) => {
                            if (resolved) return
                            resolved = true
                            this.connection.socket.removeListener('message', onMessage)
                            this.connection.socket.removeListener('error', onError)
                            try {
                                const parsed = JSON.parse(data.toString())
                                console.log(`[getThumbnail] WebSocket message #${++messageCount} received:`, JSON.stringify(parsed, null, 2))
                                msgResolve(parsed)
                            } catch (e) {
                                console.error(`[getThumbnail] Failed to parse WebSocket message:`, e.message)
                                msgReject(new Error(`Failed to parse WebSocket message: ${e.message}`))
                            }
                        }
                        
                        const onError = (err) => {
                            if (resolved) return
                            resolved = true
                            this.connection.socket.removeListener('message', onMessage)
                            this.connection.socket.removeListener('error', onError)
                            console.error(`[getThumbnail] WebSocket error:`, err)
                            msgReject(err)
                        }
                        
                        this.connection.socket.on('message', onMessage)
                        this.connection.socket.on('error', onError)
                    })
                    
                    // Check if this is the event we're waiting for AND has matching request_id
                    if (wsMessage.event === 'd2d_service_message') {
                        console.log(`[getThumbnail] Found d2d_service_message, checking request_id...`)
                        
                        // Parse the data to check request_id
                        let parsedData
                        try {
                            parsedData = JSON.parse(wsMessage.data)
                            console.log(`[getThumbnail] Parsed data event: "${parsedData.event}", request_id: "${parsedData.request_id}"`)
                        } catch (e) {
                            console.error(`[getThumbnail] Failed to parse wsMessage.data:`, e.message)
                            continue  // Skip this message, keep waiting
                        }
                        
                        // Check if this response is for our request
                        if (parsedData.request_id === id && parsedData.event === 'get_thumbnail') {
                            console.log(`[getThumbnail] MATCH! Found get_thumbnail response for request_id ${id} after ${messageCount} messages`)
                            response = wsMessage
                            break
                        } else {
                            console.log(`[getThumbnail] Request ID mismatch or wrong event. Expected request_id="${id}" and event="get_thumbnail", got request_id="${parsedData.request_id}" and event="${parsedData.event}". Continuing to wait...`)
                        }
                    } else {
                        console.log(`[getThumbnail] Not d2d_service_message (event="${wsMessage.event}"), continuing to wait...`)
                    }
                    // Otherwise, continue loop to read next message
                }
                
                clearTimeout(timeoutId)
                
                // Parse the response data
                console.log(`[getThumbnail] Parsing response data...`)
                const data = JSON.parse(response.data)
                console.log(`[getThumbnail] Parsed response.data:`, JSON.stringify(data, null, 2))
                connectionInfo = JSON.parse(data.conn_info)
                console.log(`[getThumbnail] Connection info:`, JSON.stringify(connectionInfo, null, 2))
                resolve(connectionInfo)
            } catch (error) {
                clearTimeout(timeoutId)
                console.error(`[getThumbnail] Error in event loop:`, error)
                reject(error)
            }
        })
        
        // Wait for connection info
        connectionInfo = await eventLoopPromise

        const { ip: host, port } = connectionInfo
        console.log(`[getThumbnail] Connecting d2d socket to ${host}:${port}`)

        // Open PLAIN socket connection (not TLS!) - this is critical
        const socket = new net.Socket()
        socket.setNoDelay(true)
        
        await new Promise((res, rej) => {
            socket.once('connect', () => {
                console.log(`[getThumbnail] D2D socket connected`)
                res()
            })
            socket.once('error', rej)
            socket.connect(port, host)
        })

        try {
            // Read 4-byte header length (big-endian)
            console.log(`[getThumbnail] Reading header length...`)
            const headerLengthBuffer = await new Promise((resolve, reject) => {
                const onData = (data) => {
                    console.log(`[getThumbnail] Received header length data: ${data.length} bytes`)
                    socket.off('data', onData)
                    socket.off('error', reject)
                    resolve(data)
                }
                socket.once('data', onData)
                socket.once('error', reject)
            })
            const headerLength = headerLengthBuffer.readUInt32BE(0)
            console.log(`[getThumbnail] Header length: ${headerLength} bytes`)

            // Read JSON header
            let headerData = headerLengthBuffer.slice(4)
            console.log(`[getThumbnail] Reading JSON header (${headerLength} bytes, already have ${headerData.length})...`)
            while (headerData.length < headerLength) {
                const chunk = await new Promise((resolve, reject) => {
                    const onData = (data) => {
                        console.log(`[getThumbnail] Received header chunk: ${data.length} bytes`)
                        socket.off('data', onData)
                        socket.off('error', reject)
                        resolve(data)
                    }
                    socket.once('data', onData)
                    socket.once('error', reject)
                })
                headerData = Buffer.concat([headerData, chunk])
            }

            const header = JSON.parse(headerData.slice(0, headerLength).toString('utf8'))
            console.log(`[getThumbnail] Parsed header:`, JSON.stringify(header, null, 2))
            const thumbnailLength = header.fileLength
            console.log(`[getThumbnail] Thumbnail size: ${thumbnailLength} bytes`)

            // Read thumbnail image data
            let thumbnailData = headerData.slice(headerLength)
            console.log(`[getThumbnail] Reading thumbnail data (${thumbnailLength} bytes, already have ${thumbnailData.length})...`)
            while (thumbnailData.length < thumbnailLength) {
                const chunk = await new Promise((resolve, reject) => {
                    const onData = (data) => {
                        console.log(`[getThumbnail] Received thumbnail chunk: ${data.length} bytes (total: ${thumbnailData.length + data.length}/${thumbnailLength})`)
                        socket.off('data', onData)
                        socket.off('error', reject)
                        resolve(data)
                    }
                    socket.once('data', onData)
                    socket.once('error', reject)
                })
                thumbnailData = Buffer.concat([thumbnailData, chunk])
            }

            // Close socket
            console.log(`[getThumbnail] Closing d2d socket...`)
            socket.end()
            await new Promise(res => socket.once('close', () => {
                console.log(`[getThumbnail] D2D socket closed`)
                res()
            }))

            console.log(`[getThumbnail] Successfully retrieved thumbnail: ${thumbnailData.length} bytes`)
            // Return only the thumbnail data (first thumbnailLength bytes)
            return thumbnailData.slice(0, thumbnailLength)
        } catch (error) {
            // Ensure socket is closed on error
            console.error(`[getThumbnail] D2D socket error:`, error)
            if (!socket.destroyed) {
                socket.destroy()
            }
            throw error
        }
    }
    async inArtMode() {
        const { value } = await this.request({ action: 'get_artmode_status' })
        return value === 'on'
    }
    request(...args) {
        return this.connection.request(...args)
    }
    setBrightness(value) {
        return this.request({
            action: 'set_brightness',
            value: Math.max(0, Math.min(MAX_BRIGHTNESS, Math.floor(value)))
        })
    }
    setCurrentArt({ id, category }) {
        return this.request({
            action: 'select_image',
            show: true,
            // eslint-disable-next-line
            content_id: id,
            category,
        })
    }
    async setMatte({ id, type, color }) {
        await this.request({
            action: 'change_matte',
            // eslint-disable-next-line
            content_id: id,
            // eslint-disable-next-line
            matte_id: type === 'none' ? type : `${type}_${color}`,
        })
        return this.setCurrentArt({ id })
    }
    async upload(buff, { fileType = 'png', matteType, matteColor }) {
        const date = new Date().toISOString().slice(0, 19).replace('T', ' ').replaceAll('-', ':')
        const id = randomUUID()
        // Create matte name
        const matte = matteType && matteType !== 'none' ? `${matteType}_${matteColor}` : 'none'
        // Request an open port to send the image via
        const { conn_info: connectionInfo } = await this.request({
            action: 'send_image',
            // eslint-disable-next-line camelcase
            file_type: fileType,
            id,
            // eslint-disable-next-line camelcase
            conn_info: {
                // eslint-disable-next-line camelcase
                d2d_mode: 'socket',
                // eslint-disable-next-line camelcase
                connection_id: Math.floor(Math.random() * 4 * 1024 ** 3),
                id,
            },
            // eslint-disable-next-line camelcase
            image_date: date,
            // eslint-disable-next-line camelcase
            matte_id: matte,
            // eslint-disable-next-line camelcase
            portrait_matte_id: matte,
            // eslint-disable-next-line camelcase
            file_size: buff.length
        })

        const { ip: host, port, key: secKey } = JSON.parse(connectionInfo)
        const header = {
            num: 0,
            total: 1,
            fileLength: buff.length,
            fileName: 'test',
            fileType,
            secKey,
            version: '0.0.1',
        }

        const socket = new TLSSocket()
        await new Promise(res => {
            socket.connect({ host, port, rejectUnauthorized: false }, res)
        })
        const headerMessage = Buffer.from(JSON.stringify(header), 'ascii')
        const headerSize = Buffer.alloc(4)
        headerSize.writeUInt32BE(headerMessage.length)
        await new Promise(res => socket.write(headerSize, res))
        await new Promise(res => socket.write(headerMessage, res))
        await new Promise(res => socket.write(buff, res))
        await new Promise(res => socket.end(res))

        // Wait for confirmation
        const { response: { content_id: contentID } } = await new Promise(res => this.connection.once(`response/${id}`, res))
        return contentID
    }
}
