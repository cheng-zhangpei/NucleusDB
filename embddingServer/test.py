"""
@author        :ZhangPeiCheng
@function      :
@time          :2025/11/16 13:29
"""
import os
from openai import OpenAI

client = OpenAI(
    api_key=os.environ.get('NucleusDBLLMKey'),
    base_url="https://api.deepseek.com")

response = client.chat.completions.create(
    model="deepseek-chat",
    messages=[
        {"role": "system", "content": "You are a helpful assistant"},
        {"role": "user", "content": "Hello"},
    ],
    stream=False
)

print(response.choices[0].message.content)