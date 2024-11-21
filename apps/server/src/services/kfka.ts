
import { Kafka, Producer } from "kafkajs";
import fs from "fs"
import path from "path";
import prismaClient from "./prisma";
const kafka = new Kafka({
    brokers:[""],
    ssl:{
      ca:[fs.readFileSync(path.resolve("./ca.pem"), "utf-8")]  
    },
    sasl:{
        username : "",
        password: "" ,
        mechanism:"plain"
    }
})
let producer:null | Producer = null

export async function createProducer(){
    if(producer) return producer
    const _producer = kafka.producer()
    await _producer.connect()
    producer=_producer
    return producer
}

export async function  produceMessage(message:string){
 const producer = await createProducer()
 await producer.send({
    messages:[{key:`message-${Date.now()}`,value:message}],
    topic:"MESSAGES",
 })
 return true;
}

// async function produceMessage(key:string, message:string){
//     const producer = await createProducer()
//     return () => {

//     }
// }

export async function startMessageConsumer() {
    console.log("Consumer is runnig")
    const consumer = kafka.consumer({groupId:"default"})
    await consumer.connect()
    await consumer.subscribe({topic:"MESSAGES", fromBeginning:true})

    await consumer.run({
        autoCommit:true,
        // autoCommitInterval:5
        eachMessage: async ({message,pause}) => {
            console.log(`New Message Recived..`)
            if(!message.value) return
            try {
                await prismaClient.messages.create({
                    data: {
                        text: message.value?.toString(),
                    },
                })
            } catch (error) {
               console.log("something is wrong")
               pause() 
               setTimeout(()=>{
                consumer.resume([{topic:"MESSAGES"}])
               },60*1000)
            }
        }
    })
}

export default kafka