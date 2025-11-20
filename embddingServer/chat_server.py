"""
@author        :ZhangPeiCheng
@function      :
@time          :2025/11/16 13:04
"""
# !/usr/bin/env python3
from flask import Flask, request, jsonify
from openai import OpenAI
import os
import logging
import time
from typing import Dict, Any

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)


class ChatServer:
    def __init__(self):
        self.client = None
        self.model_name = "deepseek-chat"
        self.init_client()

    def init_client(self):
        """Initialize DeepSeek client"""
        try:
            # Get API Key from environment variable
            api_key = os.getenv("NucleusDBLLMKey", "")
            if api_key == "" or api_key is None:
                logger.error("❌ NUCLEUSDBLLMKey environment variable not set")
                return
            self.client = OpenAI(
                api_key=api_key,
                base_url="https://api.deepseek.com"
            )
            logger.info(f"✅ DeepSeek chat service initialized successfully, using model: {self.model_name}")

        except Exception as e:
            logger.error(f"❌ DeepSeek client initialization failed: {e}")
            self.client = None

    def chat_completion(self, messages: list, stream: bool = False, **kwargs) -> Dict[str, Any]:
        """Generate chat completion"""
        if self.client is None:
            raise Exception("DeepSeek client not initialized")

        try:
            logger.info(f"Generating chat completion, message count: {len(messages)}, stream: {stream}")
            start_time = time.time()

            # Call DeepSeek chat API
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=messages,
                stream=stream,
                **kwargs
            )
            processing_time = time.time() - start_time
            logger.info(f"Chat completion generated, time taken: {processing_time:.2f} seconds")

            if stream:
                return response
            else:
                return {
                    'content': response.choices[0].message.content,
                    'role': response.choices[0].message.role,
                    'finish_reason': response.choices[0].finish_reason,
                    'usage': {
                        'prompt_tokens': response.usage.prompt_tokens,
                        'completion_tokens': response.usage.completion_tokens,
                        'total_tokens': response.usage.total_tokens
                    } if response.usage else None
                }

        except Exception as e:
            logger.error(f"Chat completion failed: {e}")
            raise


# Global service instance
chat_server = ChatServer()


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    status = "healthy" if chat_server.client is not None else "unhealthy"
    return jsonify({
        'status': status,
        'model': chat_server.model_name,
        'timestamp': time.time(),
        'client_initialized': chat_server.client is not None
    })


@app.route('/chat/completions', methods=['POST'])
def chat_completion():
    """Generate chat completion"""
    try:
        # Parse request data
        data = request.get_json()
        if not data:
            return jsonify({'error': 'Request body cannot be empty'}), 400

        messages = data.get('messages', [])
        stream = data.get('stream', False)
        temperature = data.get('temperature', 0.7)
        max_tokens = data.get('max_tokens', 2048)

        if not messages:
            return jsonify({'error': 'messages field cannot be empty'}), 400

        if not isinstance(messages, list):
            return jsonify({'error': 'messages must be an array'}), 400

        # Validate each message has required fields
        for i, msg in enumerate(messages):
            if 'role' not in msg or 'content' not in msg:
                return jsonify({
                    'error': f'Message {i} must have both role and content fields'
                }), 400

        logger.info(f"Received chat request, message count: {len(messages)}, stream: {stream}")

        # Generate chat completion
        if stream:
            # For streaming responses, we return the generator directly
            response = chat_server.chat_completion(
                messages=messages,
                stream=stream,
                temperature=temperature,
                max_tokens=max_tokens
            )

            def generate():
                for chunk in response:
                    if chunk.choices[0].delta.content is not None:
                        yield chunk.choices[0].delta.content

            return app.response_class(generate(), mimetype='text/plain')
        else:
            # For non-streaming responses
            result = chat_server.chat_completion(
                messages=messages,
                stream=stream,
                temperature=temperature,
                max_tokens=max_tokens
            )

            # Return results
            return jsonify({
                'choices': [{
                    'message': {
                        'role': result['role'],
                        'content': result['content']
                    },
                    'finish_reason': result['finish_reason']
                }],
                'usage': result['usage'],
                'model': chat_server.model_name,
                'timestamp': time.time()
            })

    except Exception as e:
        logger.error(f"Error processing chat request: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/models', methods=['GET'])
def list_models():
    """View available models and configurations"""
    return jsonify({
        'current_model': chat_server.model_name,
        'supported_models': [
            'deepseek-chat',
            'deepseek-coder'
        ],
        'parameters': {
            'max_tokens': 'Maximum tokens to generate (default: 2048)',
            'temperature': 'Controls randomness (0.0-1.0, default: 0.7)',
            'stream': 'Whether to stream response (true/false)'
        },
        'message_format': {
            'role': 'system|user|assistant',
            'content': 'message content'
        }
    })


@app.route('/quick-chat', methods=['POST'])
def quick_chat():
    """Quick chat interface with simplified input"""
    try:
        data = request.get_json()
        message = data.get('message', '')
        system_prompt = data.get('system_prompt', 'You are a helpful assistant')

        if not message:
            return jsonify({'error': 'message field cannot be empty'}), 400

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": message}
        ]

        logger.info(f"Quick chat request: {message[:100]}...")
        api_key = os.getenv("NucleusDBLLMKey")
        logger.info("read key=[%s]", api_key)  # 看是不是 None
        logger.info("read key=[%s***]", api_key[:8])  # 脱敏看首尾
        result = chat_server.chat_completion(messages=messages, stream=False)

        return jsonify({
            'response': result['content'],
            'usage': result['usage'],
            'timestamp': time.time()
        })

    except Exception as e:
        logger.error(f"Error in quick chat: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/conversation', methods=['POST'])
def multi_turn_chat():
    """Multi-turn conversation interface"""
    try:
        data = request.get_json()
        conversation_history = data.get('conversation', [])
        new_message = data.get('new_message', '')

        if not conversation_history and not new_message:
            return jsonify({'error': 'Either conversation history or new_message is required'}), 400

        # Build messages array
        messages = conversation_history.copy()
        if new_message:
            messages.append({"role": "user", "content": new_message})

        logger.info(f"Multi-turn chat, history: {len(conversation_history)} turns, new message: {new_message[:100]}...")

        result = chat_server.chat_completion(messages=messages, stream=False)

        # Return both the response and updated conversation history
        response_message = {
            "role": result['role'],
            "content": result['content']
        }

        updated_conversation = messages + [response_message]

        return jsonify({
            'response': result['content'],
            'updated_conversation': updated_conversation,
            'usage': result['usage'],
            'timestamp': time.time()
        })

    except Exception as e:
        logger.error(f"Error in multi-turn chat: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # Start service
    logger.info("🚀 Starting DeepSeek Chat service...")
    app.run(
        host='0.0.0.0',
        port=20001,
        debug=False,
        threaded=True
    )