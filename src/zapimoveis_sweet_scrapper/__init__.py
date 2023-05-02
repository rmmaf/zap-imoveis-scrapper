from scraper import Scraper
from typing import Union
import pandas as pd
from multiprocessing import Process
import time
from datetime import datetime
import numpy as np
def run(link:str, proxy_list_path: str, output_path: str,
        chrome_path: str, chromedriver_path:Union[str, None]=None,
        user_agent: Union[str, None]=None, implicitly_wait_time: int=30,
        page_load_timeout:int=180, max_page_limit:int=100) -> None:
    proxy_list = pd.read_csv(proxy_list_path, header=None, sep=":", names = ['ip', 'port', 'user', 'password'])
    scraper = Scraper(link, proxy_list, chrome_path, chromedriver_path, 
                      output_path, user_agent, implicitly_wait_time, 
                      page_load_timeout, max_page_limit)
    
    scraper.run()

def run_from_object(links:pd.DataFrame, proxy_list: pd.DataFrame, output_path: str,
             chrome_path: str, chromedriver_path:Union[str, None]=None,
             user_agent: Union[str, None]=None, implicitly_wait_time: int=30,
             page_load_timeout:int=180, max_page_limit:int=100) -> None:
    first_it = True
    for l in links[0]:
        if first_it:
            print(f"Scraping Link: {l}")
            scraper = Scraper(l, proxy_list, chrome_path, chromedriver_path, 
                            output_path, user_agent, implicitly_wait_time, 
                            page_load_timeout, max_page_limit)
            scraper.run()
            first_it = False
        else:
            print(f"Scraping Link: {l}")
            scraper = Scraper(l, proxy_list, chrome_path, chromedriver_path, 
                            output_path, user_agent, implicitly_wait_time, 
                            page_load_timeout, max_page_limit, first_it)
            scraper.run()

def run_list(links_csv_path:str, proxy_list_path: str, output_path: str,
             chrome_path: str, chromedriver_path:Union[str, None]=None,
             user_agent: Union[str, None]=None, implicitly_wait_time: int=30,
             page_load_timeout:int=180, max_page_limit:int=100, n_processes:int=1) -> None:
    t0 = datetime.now()
    proxy_list = pd.read_csv(proxy_list_path, header=None, sep=":", names = ['ip', 'port', 'user', 'password'])
    links = pd.read_csv(links_csv_path, sep=";", header=None)
    links = links.sample(frac=1).reset_index(drop=True)
    output_path = f'{output_path}/{time.strftime("%d_%m_%H_%M_%S", time.localtime())}_zapimoveis.csv'
    if n_processes == 1:
        run_from_object(links, proxy_list, chrome_path, chromedriver_path, 
                        output_path, user_agent, implicitly_wait_time, 
                        page_load_timeout, max_page_limit)
    else:
        
        output_paths = [output_path.replace('.csv', f'_{x}.csv') for x in range(n_processes)]
        link_split = np.array_split(links, n_processes)
        proxy_split = np.array_split(proxy_list, n_processes)
        items = list(zip(link_split, proxy_split, output_paths, [chrome_path]*n_processes, [chromedriver_path]*n_processes, 
                        [user_agent]*n_processes, [implicitly_wait_time]*n_processes, 
                        [page_load_timeout]*n_processes, [max_page_limit]*n_processes))
        
        processes = []
        for args in items:
            p = Process(target=run_from_object, args=args)
            p.start()
            processes.append(p)
        
        for p in processes:
            p.join()
        
        first_it = True
        for out_file in output_paths:
            if first_it:
                df_final = pd.read_csv(out_file, sep=";")
                first_it = False
            else:
                df_final_it = pd.read_csv(out_file, sep=";")
                df_final = pd.concat([df_final, df_final_it])
        
        df_final.drop_duplicates(inplace=True)
        df_final.to_csv(output_path, sep=";", index=False)
        print(f"Elapsed {datetime.now() - t0}")
        
if __name__ == "__main__":
    run_list(links_csv_path="C:/Users/digom/Documents/GitHub/zap-imoveis-scrapper/links_recife.csv",
        proxy_list_path="C:/Users/digom/Documents/GitHub/zap-imoveis-scrapper/proxy_list.txt",
        output_path="C:/Users/digom/Documents/GitHub/zap-imoveis-scrapper/output_files",
        chrome_path="C:/Program Files/Google/Chrome/Application/chrome.exe", n_processes=4)
    