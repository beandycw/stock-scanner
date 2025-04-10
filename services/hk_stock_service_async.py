import os
import asyncio
import pandas as pd
from typing import List, Dict, Any, Optional
from utils.logger import get_logger
from datetime import datetime, timedelta
import akshare
import requests
import math

from akshare.utils.tqdm import get_tqdm
# from akshare.utils.func import fetch_paginated_data

# 获取日志器
logger = get_logger()

class HKStockServiceAsync:
    """
    港股服务
    提供港股数据的搜索和获取功能
    """
    
    def __init__(self):
        """初始化港股服务"""
        logger.debug("初始化HKStockServiceAsync")
        
        # 可选：添加缓存以减少频繁请求
        self.cache_file = 'data/all_hk_stock.csv'
        self.cache = None
        self.cache_timestamp = None
        self.cache_duration = timedelta(days=5)

        if os.path.exists(self.cache_file):
            df = pd.read_csv(self.cache_file, dtype={'symbol': str})
            if len('日期') > 0:
                str_date = df['日期'].iloc[0]
                df.pop('日期')
                self.cache = df 
                date_format = '%Y-%m-%d %H:%M:%S'
                self.cache_timestamp = datetime.strptime(str_date, date_format)
                logger.info(f'港股缓存数据，日期：{str(self.cache_timestamp)}')
    
    async def search_stocks(self, keyword: str) -> List[Dict[str, Any]]:
        """
        异步搜索港股代码
        
        Args:
            keyword: 搜索关键词
            
        Returns:
            匹配的股票列表
        """
        try:
            logger.info(f"异步搜索港股: {keyword}")

            df = await asyncio.to_thread(self._get_all_stocks_data)

            # 模糊匹配搜索
            mask = df['name'].str.contains(keyword, case=False, na=False)
            results = df[mask]
            
            # 格式化返回结果并处理 NaN 值
            formatted_results = []
            for _, row in results.iterrows():
                formatted_results.append({
                    'name': row['name'] if pd.notna(row['name']) else '',
                    'symbol': str(row['symbol']) if pd.notna(row['symbol']) else '',
                })
                # 限制只返回前10个结果
                if len(formatted_results) >= 10:
                    break
            
            logger.info(f"港股搜索完成，找到 {len(formatted_results)} 个匹配项（限制显示前10个）")
            return formatted_results
            
        except Exception as e:
            error_msg = f"搜索港股代码失败: {str(e)}"
            logger.error(error_msg)
            logger.exception(e)
            raise Exception(error_msg)
    
    def _get_all_stocks_data(self) -> pd.DataFrame:
        """
        获取港股数据（同步方法，将被异步方法调用）
        
        Returns:
            包含港股数据的DataFrame
        """
        import akshare as ak
        
        try:
            now = datetime.now()
            if self.cache is not None and (now - self.cache_timestamp) < self.cache_duration:
                logger.debug("使用港股所有数据缓存数据")
                return self.cache
            
            logger.debug(f"从API获取港股所有数据")

            # 获取港股数据
            df = ak.stock_hk_spot_em() 
            df = df[
                [
                    "代码",
                    "名称",
                ]
            ]
            
            # 转换列名
            df = df.rename(columns={
                "名称": "name",
                "代码": "symbol",
            })

            df['日期'] = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            df.to_csv(self.cache_file, index=False)
            df.pop('日期')
            self.cache = df
            self.cache_timestamp = now
            return df
            
        except Exception as e:
            logger.error(f"获取港股数据失败: {str(e)}")
            logger.exception(e)
            raise Exception(f"获取港股数据失败: {str(e)}")
            
    async def get_stock_detail(self, symbol: str) -> Dict[str, Any]:
        """
        异步获取单个港股详细信息
        
        Args:
            symbol: 股票代码
            
        Returns:
            股票详细信息
        """
        try:
            logger.info(f"获取港股详情: {symbol}")
            
            df = await asyncio.to_thread(self._get_all_stocks_data)
            result = df.loc[df['symbol'] == symbol]
            if len(result) == 0:
                raise Exception(f"未找到股票代码{symbol}的股票简称")
            stock_name = result['name'].values[0]
            
            # 格式化为字典
            stock_detail = {
                'symbol': symbol,
                'name': stock_name
                # 'price': float(row['price']) if pd.notna(row['price']) else 0.0,
                # 'price_change': float(row['price_change']) if pd.notna(row['price_change']) else 0.0,
                # 'price_change_percent': float(row['price_change_percent'].strip('%'))/100 if pd.notna(row['price_change_percent']) else 0.0,
                # 'open': float(row['open']) if pd.notna(row['open']) else 0.0,
                # 'high': float(row['high']) if pd.notna(row['high']) else 0.0,
                # 'low': float(row['low']) if pd.notna(row['low']) else 0.0,
                # 'pre_close': float(row['pre_close']) if pd.notna(row['pre_close']) else 0.0,
                # 'market_value': float(row['market_value']) if pd.notna(row['market_value']) else 0.0,
                # 'pe_ratio': float(row['pe_ratio']) if pd.notna(row['pe_ratio']) else 0.0,
                # 'volume': float(row['volume']) if pd.notna(row['volume']) else 0.0,
                # 'turnover': float(row['turnover']) if pd.notna(row['turnover']) else 0.0
            }
            
            logger.info(f"获取港股详情成功: {symbol}")
            return stock_detail
            
        except Exception as e:
            error_msg = f"获取港股详情失败: {str(e)}"
            logger.error(error_msg)
            logger.exception(e)
            raise Exception(error_msg) 