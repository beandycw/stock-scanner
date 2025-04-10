import os
import asyncio
import pandas as pd
from typing import List, Dict, Any, Optional
from utils.logger import get_logger
from datetime import datetime, timedelta

# 获取日志器
logger = get_logger()

class USStockServiceAsync:
    """
    美股服务
    提供美股数据的搜索和获取功能
    """
    
    def __init__(self):
        """初始化美股服务"""
        logger.debug("初始化USStockServiceAsync")
        
        # 可选：添加缓存以减少频繁请求
        self.cache_file = 'data/all_us_stock.csv'
        self.cache = None
        self.cache_timestamp = None
        self.cache_duration = timedelta(days=25)

        if os.path.exists(self.cache_file):
            df = pd.read_csv(self.cache_file)
            if len('日期') > 0:
                str_date = df['日期'].iloc[0]
                df.pop('日期')
                self.cache = df 
                date_format = '%Y-%m-%d %H:%M:%S'
                self.cache_timestamp = datetime.strptime(str_date, date_format)
                logger.info(f'美股缓存数据，日期：{str(self.cache_timestamp)}')
    
    async def search_us_stocks(self, keyword: str) -> List[Dict[str, Any]]:
        """
        异步搜索美股代码
        
        Args:
            keyword: 搜索关键词
            
        Returns:
            匹配的股票列表
        """
        try:
            logger.info(f"异步搜索美股: {keyword}")
            
            df = await asyncio.to_thread(self._get_us_all_stocks_data)
            
            # 模糊匹配搜索
            mask = df['cname'].str.contains(keyword, case=False, na=False)
            results = df[mask]
            
            # 格式化返回结果并处理 NaN 值
            formatted_results = []
            for _, row in results.iterrows():
                formatted_results.append({
                    'name': row['name'] if pd.notna(row['name']) else '',
                    'cname': row['cname'] if pd.notna(row['cname']) else '',
                    'symbol': str(row['symbol']) if pd.notna(row['symbol']) else '',
                })
                # 限制只返回前10个结果
                if len(formatted_results) >= 10:
                    break
            
            logger.info(f"美股搜索完成，找到 {len(formatted_results)} 个匹配项（限制显示前10个）")
            return formatted_results
            
        except Exception as e:
            error_msg = f"搜索美股代码失败: {str(e)}"
            logger.error(error_msg)
            logger.exception(e)
            raise Exception(error_msg)
    
    def _get_us_all_stocks_data(self) -> pd.DataFrame:
        """
        获取美股数据（同步方法，将被异步方法调用）
        
        Returns:
            包含美股数据的DataFrame
        """
        import akshare as ak
    
        try:

            # 检查缓存是否有效
            now = datetime.now()
            if self.cache is not None and (now - self.cache_timestamp) < self.cache_duration:
                logger.debug("使用美股所有数据缓存数据")
                return self.cache

            logger.debug(f"从API获取美股所有数据")

            # 获取美股数据
            df = ak.get_us_stock_name()
            
            # 转换列名
            df = df.rename(columns={
                "name": "name",
                "cname": "cname",
                "symbol": "symbol"
            })

            df.set_index('symbol')
            df['日期'] = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            df.to_csv(self.cache_file, index=False)
            df.pop('日期')
            df.set_index('symbol')
            self.cache = df
            self.cache_timestamp = now
            return df
            
        except Exception as e:
            logger.error(f"获取美股所有数据失败: {str(e)}")
            logger.exception(e)
            raise Exception(f"获取美股所有数据失败: {str(e)}")
            

    def _get_us_stocks_data(self) -> pd.DataFrame:
        """
        获取美股数据（同步方法，将被异步方法调用）
        
        Returns:
            包含美股数据的DataFrame
        """
        import akshare as ak
        
        try:
            # 获取美股数据
            df = ak.stock_us_spot_em()
            
            # 转换列名
            df = df.rename(columns={
                "序号": "index",
                "名称": "name",
                "最新价": "price",
                "涨跌额": "price_change",
                "涨跌幅": "price_change_percent",
                "开盘价": "open",
                "最高价": "high",
                "最低价": "low",
                "昨收价": "pre_close",
                "总市值": "market_value",
                "市盈率": "pe_ratio",
                "成交量": "volume",
                "成交额": "turnover",
                "振幅": "amplitude",
                "换手率": "turnover_rate",
                "代码": "symbol"
            })
            
            return df
            
        except Exception as e:
            logger.error(f"获取美股数据失败: {str(e)}")
            logger.exception(e)
            raise Exception(f"获取美股数据失败: {str(e)}")
            
    async def get_us_stock_detail(self, symbol: str) -> Dict[str, Any]:
        """
        异步获取单个美股详细信息
        
        Args:
            symbol: 股票代码
            
        Returns:
            股票详细信息
        """
        try:
            logger.info(f"获取美股详情: {symbol}")
            
            df = await asyncio.to_thread(self._get_us_all_stocks_data)
            result = df.loc[df['symbol'] == symbol]
            if len(result) == 0:
                raise Exception(f"未找到股票代码{symbol}的股票简称")
            stock_name = result['cname'].values[0]
            
            # TODO API比较耗时，按需实现
            # 使用线程池执行同步的akshare调用
            # df = await asyncio.to_thread(self._get_us_stocks_data)
            # result = df[df['symbol'] == symbol]
            
            # if len(result) == 0:
            #     raise Exception(f"未找到股票代码: {symbol}")
            # row = result.iloc[0]
            
            # 格式化为字典
            stock_detail = {
                'symbol': symbol,
                'name': stock_name
                # 'enname': row['name'] if pd.notna(row['name']) else '',
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
            
            logger.info(f"获取美股详情成功: {symbol}")
            return stock_detail
            
        except Exception as e:
            error_msg = f"获取美股详情失败: {str(e)}"
            logger.error(error_msg)
            logger.exception(e)
            raise Exception(error_msg) 