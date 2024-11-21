import http from 'http'
import SocketServices from './services/socket';
import { startMessageConsumer } from './services/kfka';

async function init() {
    startMessageConsumer()
    const SocketService = new SocketServices()

    const httpServer = http.createServer();
    const PORT = process.env.PORT ? process.env.PORT : 8000
    
    SocketService.io.attach(httpServer)

    httpServer.listen(PORT,() => {
        console.log(`HTTP Server started at PORT ${PORT}`)
    })
  
    SocketService.initListeners()
}
init()