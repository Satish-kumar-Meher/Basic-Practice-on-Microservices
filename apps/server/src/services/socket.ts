import { Server } from "socket.io";
import Redis from "ioredis"
// import prismaClient from "./prisma";
import {produceMessage} from './kfka'


const pub = new Redis({
    host : "caching-170ccea8-satishmeher-redis.b.aivencloud.com",
    port : 15341,
    username:"default",
    password:""
})
const sub = new Redis({
    host : "caching-170ccea8-satishmeher-redis.b.aivencloud.com",
    port : 15341,
    username:"default",
    password:""
})
class SocketServices {
    private _io:Server
    constructor() {
        console.log("Init Socket Services...")
        this._io=new Server({
            cors : {
                allowedHeaders:["*"],
                origin:"*"
            }
        });
        sub.subscribe("MESSAGES")
       
    }

    public initListeners(){
        const io= this.io;
        console.log("init socket isteners..")
        io.on("connect",(socket)=>{
            console.log("New Socket Connected ",socket.id)

            socket.on("event:message",async ({message}:{message:any}) => {
                console.log("New message Recived",message)
                // publish the message to redis:
                await pub.publish('MESSAGES',JSON.stringify({message}))
            })
        })
        sub.on("message", async (channel,message) => {
            if(channel == "MESSAGES"){
                console.log("new message from redis",message)
                io.emit("message",message)
                // data base integration:
                // await prismaClient.messages.create({
                //     data : {
                //         text : message,
                //     }
                // })

                await produceMessage(message)
                console.log("Message produced to kafka Broker")
            }
        })
    }

    get io(){
        return this._io
    }
}

export default SocketServices