import Terry_toolkit as tkit
from terry_classify import classify
from tqdm import tqdm
import argparse
import ast
import os
from db import NodesDb
import json
 
def wiki_zh(file_path,save_all=False):
    """
    处理wikizh
    数据下载
    https://www.kaggle.com/terrychanorg/sentence-wik
    """
    tfile=tkit.File()
    flist=tfile.all_path(dirname=file_path)
    # print("flist",flist)


    output_db=tkit.Db(dbpath='../data/article.db')
    un_pet_article_db=tkit.Db(dbpath='../data/un_pet_article.db')
    tclass=classify(model_name_or_path='../model/terry_output')
    i=0
    for file in flist:
        with open(file, 'r', encoding = 'utf-8') as data:
            for it in tqdm(data):
                item=json.loads(it[:-1])
                text=item['title']+"\n"+item['text']
                # print("预测结果",)
                # print(item['title'])
                pre=tclass.pre(text)
                
                if pre==1:
                    key=hash(text) 
                    i+=1
                    output_db.add(key,{"id":key,"title":item['title'], "content":item['text']})
                    # output_data.save([{"id":key,"title":item['title'], "content":item['content']}])
                    if i%1000==0:
                        print("选择",i)
                        print(pre,item['title'])
                else:
                    # print("不是宠物内容跳过")
                    if save_all==True:
                        un_pet_article_db.add(key,{"id":key,"title":item['title'], "content":item['text']})
                    else:
                        pass


def web_text_zh(file_path,save_all=False):
    """
    数据下载
    https://www.kaggle.com/terrychanorg/webtext2019zhjsonwebtext2019zh
    https://www.kaggle.com/terrychanorg/jsonnews2016zh
    """
    # web_text_zh

    # news2016zh_valid
    # # "/media/terry/90602484602472E0/tdata/web_text_zh_valid.json"
    # data=tkit.Json(file_path=file_path).load()
    # # output_data=tkit.Json(file_path="../data/data.json")
    output_db=tkit.Db(dbpath='../data/article.db')
    un_pet_article_db=tkit.Db(dbpath='../data/un_pet_article.db')
    tclass=classify(model_name_or_path='../model/terry_output')
    i=0
    with open(file_path, 'r', encoding = 'utf-8') as data:
        for it in tqdm(data):
            item=json.loads(it[:-1])
            text=item['title']+"\n"+item['content']
            # print("预测结果",)
            pre=tclass.pre(text)
            
            if pre==1:
                key=hash(text) 
                # try:
                #     output_db.get(str(key))
                #     print("已存在")
                #     continue
                # except:
                #     pass
                i+=1
                output_db.add(key,{"id":key,"title":item['title'], "content":item['content']})
                # output_data.save([{"id":key,"title":item['title'], "content":item['content']}])
                if i%1000==0:
                    print("选择",i)
                    print(pre,item['title'])
            else:
                # print("不是宠物内容跳过")
                if save_all==True:
                    un_pet_article_db.add(key,{"id":key,"title":item['title'], "content":item['content']})
                else:
                    pass
def do_nodes(dbpath,save_all=False):
    """
    
    
    """
    db= NodesDb(db=dbpath)
    data=[]
    output_db=tkit.Db(dbpath='../data/article.db')
    if save_all==True:
        un_pet_article_db=tkit.Db(dbpath='../data/un_pet_article.db')
    tclass=classify(model_name_or_path='../model/terry_output')
    i=0
    for  id,title,content,author,url,label in tqdm(db.get_all_nodes(limit=1000000000000)):
        text=title+"\n"+content
        # text= text[:512]
        # print("预测结果",)
        # print(len(text[:512]))
        pre=tclass.pre(text) #预测
        if pre==1:
            key=hash(text) 
            i+=1
            # print(i)
            output_db.add(key,{"id":key,"title":title, "content":content})#保存数据
            if i%1000==0:
                print("选择",i)
                print(pre,title)
        else:
            # print("不是宠物内容跳过")
            if save_all==True:
                un_pet_article_db.add(key,{"id":key,"title":title, "content":content})#保存非宠物数据
            else:
                pass

            

def get():
    output_db=tkit.Db(dbpath='../data/article.db')
    items=[item for item in output_db.db]
    # print(items)
    print(output_db.get('9027335983622895200'))
    print(len(items))

def all_json():
 
    # items=[item for item in output_db.db]
    if os.path.exists('../data/data.json'):
        print('文件存在请删除后执行　../data/data.json')
        return
    else:
        print("开始执行数据转化为json")
        output_db=tkit.Db(dbpath='../data/article.db')
        output_data=tkit.Json(file_path="../data/data.json")
        for item in output_db.db:
            # print(item)
            value=str(item[1],"utf-8")
            value=ast.literal_eval(value)
            # print(value)
            output_data.save([value])
            # try:
                
            #     print(item)
            # except:
            #     pass
        
        # print(len(items))
    # get()

def main():
    parser = argparse.ArgumentParser(usage="运行数据构建.", description="help info.")
    parser.add_argument("--do", type=str, default='get',required=False, help="输入运行的类型  ( web_text_zh,all_json,get)")
    parser.add_argument("--file", type=str, default='get',required=False, help="输入运行的类型  ( web_text_zh(处理 https://www.kaggle.com/terrychanorg/webtext2019zhjsonwebtext2019zh)\n　get, )")
    parser.add_argument("--save_all", type=str, default=False,required=False, help="是否保存非宠物数据　默认False")

    args = parser.parse_args()
    if args.do == 'web_text_zh':
        web_text_zh(file_path=args.file,save_all=args.save_all)
    elif args.do == 'do_nodes':
        do_nodes(dbpath=args.file,save_all=args.save_all)
    elif args.do == 'get':
        get()
    elif args.do == 'all_json':
        all_json()
    elif args.do == 'wiki_zh':
        wiki_zh(file_path=args.file,save_all=args.save_all)

if __name__ == '__main__':
    main()