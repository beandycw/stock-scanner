from google import genai
import re
import os

technical_summary = {
        'trend': 'upward',
        'volatility': f"10%",
        'volume_trend': 'increasing',
        'rsi_level': 50
    }

prompt = f"""
        分析A股 000001：
        
        术指标概要：
        {technical_summary}
                
        请提供：
        1. 趋势分析（包含支撑位和压力位）
        2. 成交量分析及其含义
        3. 风险评估（包含波动率分析）
        4. 短期和中期目标价位
        5. 关键技术位分析
        6. 具体交易建议（包含止损位）
        
        请基于技术指标和A股市场特点进行分析，给出具体数据支持。
        """

stream =True
API_KEY = "your google genai api key"
client = genai.Client(api_key=API_KEY)

if stream:
    response = client.models.generate_content_stream(model="gemini-2.0-flash", contents=prompt)
    for chunk in response:
        print(chunk.text)
        print("_" * 80)
else:
    response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
    print(response.text)    
