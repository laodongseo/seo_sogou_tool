# ‐*‐ coding: utf‐8 ‐*‐
"""
功能:
   1)指定几个【顶级】域名,分关键词种类监控首页词数
   2)搜狗PC排序是从0开始的
提示:
  1)不含搜了还搜,含微信和搜狗百科
  2)搜索结果不太规范，提取排名值需从3种标签的id属性获取
  3)a或div.r-sech或者搜索结果出图div
结果:
	sgpc1_index_info.txt:各监控站点词的排名及url,如有2个url排名,只取第一个
	sgpc1_index_all.txt:结果页所有url
"""

import requests,uuid
from pyquery import PyQuery as pq
import threading
import queue
import time
from urllib.parse import urlparse
import time
import gc
import random
import re
import tld
import traceback
import pandas as pd
import copy
from itertools import chain
requests.packages.urllib3.disable_warnings()


my_header = {
	'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
	'Accept-Encoding': 'gzip, deflate',
	'Accept-Language': 'zh-CN,zh;q=0.9',
	'Connection': 'keep-alive',
	'Host': 'www.sogou.com',
	'Referer':'https://www.sogou.com/sie?ie=utf8&query=%E5%A6%82%E4%BD%95%E5%8C%BA%E5%88%86%E5%B8%B8%E8%A7%81USB&pid=AWNb5-0000',
	'Sec-Fetch-Dest': 'document',
	'Sec-Fetch-Mode': 'navigate',
	'Sec-Fetch-Site': 'same-origin',
	'Sec-Fetch-User': '?1',
	'Upgrade-Insecure-Requests': '1',
	'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110.' + str(uuid.uuid4()) + ' Safari/537.36',
}


def get_cookie(num):
	if num == 1:
		r = requests.get(url="https://www.sogou.com",headers=my_header)
	else:
		r = requests.get(url="https://v.sogou.com/v?ie=utf8&query=&p=40030600",headers=my_header)
	cookie = ";".join([f'{key}={value}' for key, value in r.cookies.items()])
	return cookie


class sgpcIndexMonitor(threading.Thread):

	def __init__(self):
		threading.Thread.__init__(self)

	@staticmethod
	def read_excel(filepath):
		q = queue.Queue()
		df_dict = pd.read_excel(filepath,sheet_name=None)
		for sheet_name,df_sheet in df_dict.items():
			values = df_sheet['kwd'].dropna().values
			for kwd in values:
				if str(kwd).strip():
					q.put((sheet_name,kwd))
		return q

	# 获取某词serp源码
	def get_html(self,url,retry=1):
		headers = copy.deepcopy(my_header)
		headers['Cookie'] = get_cookie(retry)
		try:
			r = requests.get(url=url,headers=headers,verify=False,timeout=15)
		except Exception as e:
			print('获取源码失败',e)
			time.sleep(30)
			if retry > 0:
				self.get_html(url,retry-1)
		else:
			html = r.content.decode('utf-8',errors='ignore')
			url = r.url
			return html,url

	# 解析源码提取加密url及rank
	def get_encrpt_urls(self,html):
		encrypt_url_list = []
		doc = pq(html)
		div_mode_list = doc('div.results div.vrwrap').items()
		div_label_list = doc('div.results div.rb').items() # 描述出[xx]
		div_list = chain(div_mode_list,div_label_list)
		for div in div_list:
			h3 = div('h3') # 有时候class=vr-title带空格
			a = h3('a:eq(0)')
			# 搜狗百科样式
			if not a:
				a = h3.parent('a')
			attr_id = a.attr('id')
			if not attr_id:
				attr_id = div('div.r-sech').attr('id')
			if not attr_id: # 搜索结果出图
				attr_id = div('div.img-layout a').attr('id')
			attr_id = re.sub('\"|\'','',attr_id) if attr_id else None
			rank = int(attr_id.split('_')[-1]) if attr_id else None
			encrypt_url = a.attr('href') if a else None
			if encrypt_url and isinstance(rank,int):
				encrypt_url_list.append((encrypt_url,rank))
		return encrypt_url_list

	# 解密某条加密url
	def decrypt_url(self,encrypt_url,retry=1):
		# 显式url
		if encrypt_url.startswith('http'):
			real_url = encrypt_url
			return real_url
		# 搜狗合作形式
		if 'javascript:' in f'{encrypt_url}':
			return 'https://www.sogou.com/'
		encrypt_url = f'https://www.sogou.com{encrypt_url}'
		html_now_url = self.get_html(encrypt_url)
		html,now_url = html_now_url if html_now_url else ('','')
		js_html = html.replace('\n','')
		# print('---',js_html)
		res = re.search(r'content.*URL=\'(.*?)\'"></noscript>',js_html,re.S|re.I)
		real_url = res.group(1) if res else 'xxx'
		return real_url

	# 提取url顶级域名
	def get_top_domain(self,real_url):
		top_domain = ''
		try:
			obj = tld.get_tld(real_url,as_object=True)
			top_domain = obj.fld
		except Exception as e:
			print('top domain parse error:',e)
		return top_domain

	# 首页排名url转为顶级域名信息
	def get_top_domains(self,real_urls_rank):
		domain_url_dicts = {}
		for real_url, my_order in real_urls_rank:
			if real_url:
				top_domain = self.get_top_domain(real_url)
				# 一个词某域名多个url有排名,算一次
				domain_url_dicts[top_domain] = (real_url,my_order) if top_domain not in domain_url_dicts else domain_url_dicts[top_domain]
		return domain_url_dicts

	# 保存结果页所有数据
	def save_serp(self,kwd,group,encrypt_url_list_rank):
		real_urls_rank = []
		for serp_url,my_order in encrypt_url_list_rank:
			# print(serp_url)
			real_url = self.decrypt_url(serp_url)
			real_urls_rank.append((real_url,my_order))
			Lock.acquire()
			f_all.write(f'{kwd}\t{real_url}\t{my_order}\t{group}\n')
			Lock.release()
			time.sleep(0.3)
		f_all.flush()
		return real_urls_rank

	# 保存目标域名结果
	def save(self,kwd,group,domain_url_dicts):
		# 目标域名是否出现
		for domain in TargetDomains:
			if domain not in domain_url_dicts:
				Lock.acquire()
				f.write(f'{kwd}\t无\t无\t{group}\t{domain}\n')
				Lock.release()
			else:
				my_url,my_order = domain_url_dicts[domain]
				Lock.acquire()
				f.write(f'{kwd}\t{my_url}\t{my_order}\t{group}\t{domain}\n')
				Lock.release()
		f.flush()

	# 线程函数
	def run(self):
		while 1:
			group_kwd = q.get()
			group,kwd = group_kwd
			print(group,kwd)
			try:
				url = f"https://www.sogou.com/web?ie=utf8&query={kwd}"
				html_now_url = self.get_html(url)
				if not html_now_url:
					q.put(group_kwd)
					continue
				html,now_url = html_now_url
				re_obj = re.search('<title>(.*?)</title>',html,re.S|re.I)
				title = re_obj.group(1) if re_obj else ''
				if '搜狗搜索' not in title or 'https://www.sogou.com/web?ie=utf8&query' not in now_url:
					q.put(group_kwd)
					print('sleep...',now_url)
					time.sleep(120)
					continue
				encrypt_url_list_rank = self.get_encrpt_urls(html)
			except Exception as e:
				print(traceback.format_exc())
				traceback.print_exc(file=open(f'{today}log.txt', 'a'))
			else:
				print(len(encrypt_url_list_rank))
				real_urls_rank = self.save_serp(kwd,group,encrypt_url_list_rank)
				# {'域名':(real_url,my_order)}
				domain_url_dicts = self.get_top_domains(real_urls_rank)
				self.save(kwd,group,domain_url_dicts)
			finally:
				del kwd
				gc.collect()
				q.task_done()
				time.sleep(3)


if __name__ == "__main__":
	start = time.time()
	today = time.strftime('%Y%m%d',time.localtime())
	list_headers = [i.strip() for i in open('ua_pc.txt','r',encoding='utf-8')]
	TargetDomains = ['5i5j.com','lianjia.com','ke.com','anjuke.com','fang.com'] # 目标域名
	q = sgpcIndexMonitor.read_excel('2021kwd_url_core_city.xlsx')  # 关键词队列及分类
	all_num = q.qsize() # 总词数
	f = open('{0}sgpc1_index_info.txt'.format(today),'w',encoding="utf-8")
	f_all = open('{0}sgpc1_index_all.txt'.format(today),'w',encoding="utf-8")
	Lock = threading.Lock()
	# 设置线程数
	for i in list(range(1)):
		t = sgpcIndexMonitor()
		t.setDaemon(True)
		t.start()
	q.join()
	f.close()
	f_all.close()
	# 统计查询成功的词数
	with open(f.name,'r',encoding='utf-8') as fp:
		success = int(sum(1 for x in fp)/len(TargetDomains))
	end = time.time()
	print('关键词共{0}个,查询成功{1}个,耗时{2}min'.format(all_num,success,(end - start) / 60))
