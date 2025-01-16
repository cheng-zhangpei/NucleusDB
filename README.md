# ComDB - A NoSQL Database Based on Bitcask Model

## Overview

​	ComDB is a key-value storage engine based on the Bitcask data model, designed for memory storage and search in multi-agent systems. It provides high-performance read/write operations, low latency, and high throughput, supporting data processing beyond memory capacity. ComDB also integrates search functionality to enable efficient memory space search for agents.

​	The author's capabilities are limited, and this database is primarily a learning project. Many features are still under development and not fully refined. For instance, while the merge operation has been implemented, it currently lacks automated scheduled cleanup. Additionally, the transaction serialization mechanism is relatively simplistic, resulting in limited transaction throughput. Furthermore, numerous functionalities have not undergone thorough testing. Therefore, please avoid using this database in critical production environments. Ongoing improvements and enhancements will be made over time.

[document](https://github.com/cheng-zhangpei/ComDB/tree/main/doc/doc.md)

## Features

- **High Performance**: Low-latency read/write operations and high throughput.
- **Memory Efficiency**: Handles data larger than memory capacity.
- **Search Integration**: Built-in search functionality for efficient memory space search.
- **Redis Compatibility**: Supports Redis data structures (String, Hash, Set, List, ZSet, Bitmap) and RESP protocol.

## Prerequisites

- Go 1.16 or higher.
- Linux environment (recommended for deployment).

## Getting Started

### Redis Support

```sh
git clone https://github.com/cheng-zhangpei/ComDB.git
cd ComDB/main
./main
```

​	it will open a server which support data operations by redis-cli or redis SDK in different lanuages

if you install redis-cli, you can test the function easily

![test](../image/redis-test.png)

#### Http Server

```sh
git clone https://github.com/cheng-zhangpei/ComDB.git
cd ComDB/http
./http
```

​	it will open a http server for some data operation. you can see the doc for details.[document](https://github.com/cheng-zhangpei/ComDB/tree/main/doc/doc.md)

#### Example For Developer

```go
import (
	"ComDB"
	"fmt"
)

func main() {
	opts := ComDB.DefaultOptions
	opts.DirPath = "/tmp/bitcask-go"
	db, err := ComDB.Open(opts)
	if err != nil {
		panic(err)
	}
	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val))
}
```

​	The details of the http server interface when you boot the execution file in http directory.You can use the interface with /memory prefix to build your llm memory space or even build your mult-agent-cooperation framework !  Now the function is a initial version, more functions,higher availiablity will come soon (if I have time hahh, but it is hard)

[http_interface_details](https://github.com/cheng-zhangpei/ComDB/tree/main/doc/interface.md)

```python
pip install comdb_client
#-----------------------------
from comdb_client import ComDBClient


#  Example usage
if __name__ == "__main__":
    client = ComDBClient("172.31.88.128", 9090)

    # Test the connection
    if client.test_connection():
        print("Connection successful!")
    else:
        print("Failed to connect to ComDB server.")

    # Perform some operations
    if client.put("czp", "ZhangPeiCheng"):
        print("Successfully stored the key-value pair.")

    value = client.get("czp")
    if value:
        print(f"Retrieved value: {value}")
    else:
        print("Failed to retrieve the value.")

    keys = client.listKey()
    print(keys)

    client.put("czp1", "ZhangPeiCheng1")
    client.put("czp2", "ZhangPeiCheng2")
    client.put("czp3", "ZhangPeiCheng3")
    client.put("czp4", "ZhangPeiCheng4")
    result = client.get_by_prefix("czp")
    print(result)

    agentId = "114514"
    # # insert into the memory 
    text_message = [
        "In the field of artificial intelligence, training deep learning models requires a large amount of data and computational resources. To improve model performance, researchers often use distributed training techniques, distributing tasks across multiple GPUs or TPUs for parallel processing. This approach can significantly reduce training time, but it also introduces challenges related to data synchronization and communication overhead.",
        "Natural Language Processing (NLP) is an important branch of artificial intelligence, focusing on enabling computers to understand and generate human language. In recent years, Transformer-based models (such as BERT and GPT) have made significant progress in NLP tasks. These models capture contextual information in text through self-attention mechanisms, leading to outstanding performance in various tasks.",
        "Cloud computing is a core component of modern IT infrastructure, allowing users to access computing resources, storage, and applications over the internet. Cloud service providers (such as AWS, Azure, and Google Cloud) offer elastic scaling and pay-as-you-go models, enabling businesses to manage and deploy their IT resources more efficiently.",
        "Blockchain technology is a decentralized distributed ledger technology widely used in cryptocurrencies (such as Bitcoin and Ethereum). Blockchain ensures data security and immutability through cryptographic algorithms, while achieving decentralized trust mechanisms through consensus mechanisms (such as PoW and PoS).",
        "The Internet of Things (IoT) refers to connecting various physical devices through the internet, enabling them to communicate and collaborate with each other. IoT technology has broad applications in smart homes, industrial automation, and smart cities. Through sensors and data analysis, IoT helps businesses and individuals achieve more efficient resource management and decision-making support.",
        "Quantum computing is a computational model based on the principles of quantum mechanics, with the potential to surpass classical computers. Quantum bits (qubits) can exist in multiple states simultaneously, allowing quantum computers to process large amounts of data in parallel. Although quantum computing is still in its early stages, it shows great promise in fields such as cryptography, materials science, and drug development.",
        "Edge computing is a computing paradigm that shifts computational tasks from centralized cloud systems to edge devices closer to the data source. Edge computing can reduce data transmission latency, improve real-time performance, and lower bandwidth consumption. It has significant applications in autonomous driving, industrial IoT, and smart cities.",
        "Data science is an interdisciplinary field that combines statistics, computer science, and domain knowledge to extract valuable insights from data. Data scientists use machine learning, data mining, and visualization tools to analyze data, helping businesses make data-driven decisions. Data science has wide applications in finance, healthcare, and marketing.",
        "DevOps is a software development methodology that emphasizes collaboration and automation between development and operations teams. Through continuous integration, continuous delivery, and automated testing, DevOps can significantly improve the efficiency and quality of software development. The DevOps culture also encourages rapid iteration and continuous improvement to meet rapidly changing market demands.",
        "Reinforcement learning is a machine learning approach that trains agents through trial and error and reward mechanisms. Reinforcement learning has broad applications in game AI, robotics control, and autonomous driving. Unlike supervised and unsupervised learning, reinforcement learning does not require pre-labeled data but learns optimal strategies through interaction with the environment."
    ]

    # All matching messages are placed in a list in parallel
    match_message = [
        "Distributed training techniques for deep learning models can significantly reduce training time but also introduce challenges related to data synchronization and communication overhead. Optimizing the efficiency of distributed training is an important research direction.",
        "Transformer models in natural language processing capture contextual information in text through self-attention mechanisms, leading to outstanding performance in various tasks. BERT and GPT are representative models in this field.",
        "Cloud computing enables businesses to manage and deploy their IT resources more efficiently through elastic scaling and pay-as-you-go models. AWS, Azure, and Google Cloud are major cloud service providers.",
        "Blockchain technology ensures data security and immutability through cryptographic algorithms, while achieving decentralized trust mechanisms through consensus mechanisms. Bitcoin and Ethereum are typical applications of blockchain technology.",
        "IoT technology helps businesses and individuals achieve more efficient resource management and decision-making support through sensors and data analysis. Smart homes and smart cities are important application scenarios for IoT.",
        "Quantum computing, based on the principles of quantum mechanics, has the potential to surpass classical computers. Quantum bits can exist in multiple states simultaneously, enabling quantum computers to process large amounts of data in parallel."
    ]

    if client.create_memory_meta(agentId,10):
        print("create successfully")
    for message in text_message:
        if client.memory_set(agentId, message) is False:
            print("memory message insert error")
    memory = client.memory_get(agentId)
    print(memory)
    for test in match_message:
        result = client.memory_search(agentId, test)
        print(result)

    client.compress_memory(agentId,"http://172.24.216.71:5000/generate")
```

Any question please email:   chengzipi@jmu.edu.cn