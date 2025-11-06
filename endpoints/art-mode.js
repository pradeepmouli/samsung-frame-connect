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
        
        this.connection.socket.send(JSON.stringify(message))
        
        // Implement event loop to wait for d2d_service_message event
        // (Python reference: _send_art_request in samsungtvws/art.py)
        let response
        let connectionInfo
        const timeout = setTimeout(() => {
            throw new Error('Timeout waiting for d2d_service_message event')
        }, this.connection.responseTimeout)
        
        try {
            // Keep reading WebSocket messages until we get d2d_service_message
            while (true) {
                const message = await new Promise((resolve, reject) => {
                    const onMessage = (data) => {
                        this.connection.socket.off('message', onMessage)
                        this.connection.socket.off('error', reject)
                        try {
                            resolve(JSON.parse(data.toString()))
                        } catch (e) {
                            reject(new Error(`Failed to parse WebSocket message: ${e.message}`))
                        }
                    }
                    this.connection.socket.once('message', onMessage)
                    this.connection.socket.once('error', reject)
                })
                
                // Check if this is the event we're waiting for
                if (message.event === 'd2d_service_message') {
                    response = message
                    break
                }
                
                // Otherwise, continue loop to read next message
            }
            
            clearTimeout(timeout)
            
            // Parse the response data
            const data = JSON.parse(response.data)
            connectionInfo = JSON.parse(data.conn_info)
        } catch (error) {
            clearTimeout(timeout)
            throw error
        }

        const { ip: host, port } = connectionInfo

        // Open PLAIN socket connection (not TLS!) - this is critical
        const socket = new net.Socket()
        await new Promise((res, rej) => {
            socket.connect(port, host, res)
            socket.once('error', rej)
        })

        // Read 4-byte header length (big-endian)
        const headerLengthBuffer = await new Promise((resolve, reject) => {
            const onData = (data) => {
                socket.off('data', onData)
                socket.off('error', reject)
                resolve(data)
            }
            socket.once('data', onData)
            socket.once('error', reject)
        })
        const headerLength = headerLengthBuffer.readUInt32BE(0)

        // Read JSON header
        let headerData = headerLengthBuffer.slice(4)
        while (headerData.length < headerLength) {
            const chunk = await new Promise((resolve, reject) => {
                const onData = (data) => {
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
        const thumbnailLength = header.fileLength

        // Read thumbnail image data
        let thumbnailData = headerData.slice(headerLength)
        while (thumbnailData.length < thumbnailLength) {
            const chunk = await new Promise((resolve, reject) => {
                const onData = (data) => {
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
        await new Promise(res => socket.end(res))

        // Return only the thumbnail data (first thumbnailLength bytes)
        return thumbnailData.slice(0, thumbnailLength)
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
