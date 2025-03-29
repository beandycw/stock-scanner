import asyncio
import pandas as pd
from typing import List, Dict, Any, Optional
from utils.logger import get_logger
from datetime import datetime, timedelta

# 获取日志器
logger = get_logger()

class AStockServiceAsync:
    """
    A股服务
    提供A股数据的搜索和获取功能
    """
    
    def __init__(self):
        """初始化A股服务"""
        logger.debug("初始化AStockServiceAsync")
        
        # 可选：添加缓存以减少频繁请求
        self.cache = None
        self.cache_timestamp = None
        self.cache_duration = timedelta(days=5)
    
    async def search_stocks(self, keyword: str) -> List[Dict[str, Any]]:
        """
        异步搜索A股代码
        
        Args:
            keyword: 搜索关键词
            
        Returns:
            匹配的股票列表
        """
        try:
            logger.info(f"异步搜索A: {keyword}")
            
                        # 检查缓存是否有效
            now = datetime.now()
            if self.cache is None or (now - self.cache_timestamp) > self.cache_duration:
                # 使用线程池执行同步的akshare调用
                logger.debug(f"从API获取A股所有数据")
                df = await asyncio.to_thread(self._get_all_stocks_data)
                self.cache = df
                self.cache_timestamp = now
            else:
                df = self.cache
                logger.debug("使用A股所有数据缓存数据")

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
            
            logger.info(f"A股搜索完成，找到 {len(formatted_results)} 个匹配项（限制显示前10个）")
            return formatted_results
            
        except Exception as e:
            error_msg = f"搜索A股代码失败: {str(e)}"
            logger.error(error_msg)
            logger.exception(e)
            raise Exception(error_msg)
    
    def _get_all_stocks_data(self) -> pd.DataFrame:
        """
        获取A股数据（同步方法，将被异步方法调用）
        
        Returns:
            包含A股数据的DataFrame
        """
        import akshare as ak
        
        try:
            # 获取A股数据
            df = ak.stock_info_a_code_name()
            
            # 转换列名
            df = df.rename(columns={
                "code": "symbol",
                "name": "name",
            })

            # 删除 'name' 列中字符串开头和结尾的空格
            df['name'] = df['name'].str.replace(' ', '')
            return df
            
        except Exception as e:
            logger.error(f"获取A数据失败: {str(e)}")
            logger.exception(e)
            raise Exception(f"获取A数据失败: {str(e)}")

    def _get_stocks_data_detail(self, symbol: str) -> pd.DataFrame:
        """
        获取指定A股的数据（同步方法，将被异步方法调用）
        
        Returns:
            包含A股数据的DataFrame
        """
        import akshare as ak
        
        try:
            # 获取A股数据
            df = ak.stock_individual_info_em(symbol=symbol)
            
            # 转换列名
            # TODO: 需要根据实际情况转换列名
            df = df.rename(columns={
                "code": "symbol",
                "name": "name",
            })
            
            return df
            
        except Exception as e:
            logger.error(f"获取A数据失败: {str(e)}")
            logger.exception(e)
            raise Exception(f"获取A数据失败: {str(e)}")


    async def get_stock_detail(self, symbol: str) -> Dict[str, Any]:
        """
        异步获取单个A股详细信息
        
        Args:
            symbol: 股票代码
            
        Returns:
            股票详细信息
        """
        try:
            logger.info(f"获取A股详情: {symbol}")
            
            # 使用线程池执行同步的akshare调用
            df = await asyncio.to_thread(self._get_stocks_data_detail)
            
            # 精确匹配股票代码
            result = df[df['symbol'] == symbol]
            
            if len(result) == 0:
                raise Exception(f"未找到股票代码: {symbol}")
            
            # 获取第一行数据
            row = result.iloc[0]
            
            # 格式化为字典
            stock_detail = {
                'name': row['name'] if pd.notna(row['name']) else '',
                'symbol': str(row['symbol']) if pd.notna(row['symbol']) else '',
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
            
            logger.info(f"获取A股详情成功: {symbol}")
            return stock_detail
            
        except Exception as e:
            error_msg = f"获取A股详情失败: {str(e)}"
            logger.error(error_msg)
            logger.exception(e)
            raise Exception(error_msg) 