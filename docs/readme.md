### **ʜuɠɘɗata**
> IT4931: soict - hust
---  

`kubectl apply -f k8s/namespace.yaml`  
<!-- `kubectl apply -f kafka/zookeeper.yaml`   -->
`kubectl apply -f kafka/kafka.yaml`  

**dev**  - kafka localhost:9094  
`kubectl port-forward pods/kafka-0 9094:9094 -n hugedata`     

**Overview Architecture**: 
> https://www.canva.com/design/DAG3tcajdLA/1BPAGW-9S1AUOJAQAVEIEw/edit?utm_content=DAG3tcajdLA&utm_campaign=designshare&utm_medium=link2&utm_source=sharebutton  

![alt text](image.png)