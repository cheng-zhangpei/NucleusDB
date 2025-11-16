#!/usr/bin/env python3
from flask import Flask, request, jsonify
from openai import OpenAI
import os
import logging
import time
from typing import List, Optional
import numpy as np

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)


class AliEmbeddingServer:
    def __init__(self):
        self.client = None
        self.model_name = "text-embedding-v4"
        self.default_dimensions = 1024  # Default dimensions
        self.init_client()

    def init_client(self):
        """Initialize Alibaba Cloud client"""
        try:
            # Get API Key from environment variable
            base_url = os.getenv("DASHSCOPE_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
            api_key = os.getenv("EMBEDDINGCLIENT", "")
            self.client = OpenAI(
                api_key=api_key,
                base_url=base_url
            )
            logger.info(f"✅ Alibaba Cloud embedding service initialized successfully, using model: {self.model_name}")

        except Exception as e:
            logger.error(f"❌ Alibaba Cloud client initialization failed: {e}")
            self.client = None

    def embed_texts(self, texts: List[str], dimensions: Optional[int] = None) -> List[List[float]]:
        """Generate text embedding vectors"""
        if self.client is None:
            raise Exception("Alibaba Cloud client not initialized")

        if dimensions is None:
            dimensions = self.default_dimensions

        try:
            logger.info(f"Generating embedding vectors, text count: {len(texts)}, dimensions: {dimensions}")
            start_time = time.time()

            # Call Alibaba Cloud embedding API
            response = self.client.embeddings.create(
                model=self.model_name,
                input=texts,
                dimensions=dimensions
            )
            processing_time = time.time() - start_time
            logger.info(f"Embedding generation completed, time taken: {processing_time:.2f} seconds")

            # Extract embedding vectors
            embeddings = [item.embedding for item in response.data]
            return embeddings

        except Exception as e:
            logger.error(f"Embedding generation failed: {e}")
            raise


# Global service instance
embedding_server = AliEmbeddingServer()


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    status = "healthy" if embedding_server.client is not None else "unhealthy"
    return jsonify({
        'status': status,
        'model': embedding_server.model_name,
        'timestamp': time.time(),
        'client_initialized': embedding_server.client is not None
    })


@app.route('/embed', methods=['POST'])
def embed_text():
    """Generate text embedding vectors"""
    try:
        # Parse request data
        data = request.get_json()
        if not data:
            return jsonify({'error': 'Request body cannot be empty'}), 400

        texts = data.get('texts', [])
        dimensions = data.get('dimensions', 1024)  # Default 1024 dimensions

        if not texts:
            return jsonify({'error': 'texts field cannot be empty'}), 400

        if not isinstance(texts, list):
            return jsonify({'error': 'texts must be a string array'}), 400

        # Validate dimensions parameter
        valid_dimensions = [64, 128, 256, 512, 768, 1024, 1536, 2048]
        if dimensions not in valid_dimensions:
            return jsonify({
                'error': f'Dimensions must be one of: {valid_dimensions}'
            }), 400

        logger.info(f"Received embedding request, text count: {len(texts)}, requested dimensions: {dimensions}")

        # Generate embedding vectors
        embeddings = embedding_server.embed_texts(texts, dimensions)

        # Return results
        return jsonify({
            'embeddings': embeddings,
            'dimension': dimensions,
            'text_count': len(texts),
            'model': embedding_server.model_name,
            'timestamp': time.time()
        })

    except Exception as e:
        logger.error(f"Error processing embedding request: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/models', methods=['GET'])
def list_models():
    """View available models and configurations"""
    valid_dimensions = [64, 128, 256, 512, 768, 1024, 1536, 2048]

    return jsonify({
        'current_model': embedding_server.model_name,
        'supported_models': [
            'text-embedding-v4',
            'text-embedding-v3',
            'text-embedding-v2',
            'qwen2.5-vl-embedding'  # Multimodal model
        ],
        'supported_dimensions': valid_dimensions,
        'default_dimensions': embedding_server.default_dimensions,
        'batch_size_limit': 10,  # Maximum 10 texts per request
        'token_limit': 8192,  # Maximum 8192 tokens per text
        'languages': [
            'Chinese', 'English', 'Spanish', 'French', 'Portuguese',
            'Indonesian', 'Japanese', 'Korean', 'German', 'Russian and 100+ languages'
        ]
    })


@app.route('/batch-test', methods=['POST'])
def batch_test():
    """Batch testing interface"""
    try:
        data = request.get_json()
        test_texts = data.get('texts', [])

        if not test_texts:
            test_texts = [
                "I like it, will buy here again",
                "Product quality is good, fast shipping",
                "Not satisfied, packaging was damaged",
                "Customer service attitude is good, problem solved quickly",
                "Good value for money, will recommend to friends"
            ]

        results = {}

        # Test different dimensions
        for dim in [256, 512, 1024]:
            try:
                start_time = time.time()
                embeddings = embedding_server.embed_texts(test_texts, dim)
                processing_time = time.time() - start_time

                results[f'dim_{dim}'] = {
                    'success': True,
                    'processing_time': processing_time,
                    'embedding_shape': f"{len(embeddings)}x{len(embeddings[0])}",
                    'sample_embedding': embeddings[0][:5]  # Show first 5 values
                }
            except Exception as e:
                results[f'dim_{dim}'] = {
                    'success': False,
                    'error': str(e)
                }

        return jsonify({
            'test_results': results,
            'test_texts': test_texts
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/similarity', methods=['POST'])
def calculate_similarity():
    """Calculate text similarity"""
    try:
        data = request.get_json()
        text1 = data.get('text1', '')
        text2 = data.get('text2', '')
        dimensions = data.get('dimensions', 1024)

        if not text1 or not text2:
            return jsonify({'error': 'text1 and text2 cannot be empty'}), 400

        # Generate embedding vectors
        embeddings = embedding_server.embed_texts([text1, text2], dimensions)
        vec1 = np.array(embeddings[0])
        vec2 = np.array(embeddings[1])

        # Calculate cosine similarity
        cosine_sim = np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))

        return jsonify({
            'text1': text1,
            'text2': text2,
            'similarity': float(cosine_sim),
            'dimension': dimensions,
            'timestamp': time.time()
        })

    except Exception as e:
        logger.error(f"Error calculating similarity: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # Check environment variables
    api_key = "sk-2b3e4f939b734798977081f73b83ad2f"

    # Start service
    logger.info("🚀 Starting Alibaba Cloud embedding service...")
    app.run(
        host='0.0.0.0',
        port=20002,
        debug=False,
        threaded=True
    )