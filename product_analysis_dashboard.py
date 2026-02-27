import streamlit as st
import pandas as pd
import plotly.express as px
import mysql.connector
from mysql.connector import Error
import time

# 配置信息
db_config = {
    "host": "10.8.36.88",
    "port": 33061,
    "user": "read_bi_dmops",
    "password": "HgXEELLzEG8Ex#",
    "database": "bi"
}

# SQL查询语句
query = """
SELECT 
  rg.relation_code AS "产品编码", 
  main_project_group_name as "主项目组", 
  status as "产品销售状态", 
  cinvproductstatus as "产品状态", 
  pur.threecategory_name_exhibition AS "末级品类", 
  category_attribute as "产品属性名称"
FROM 
  bi.replace_group_bi_apply rg 
  LEFT JOIN (
  SELECT 
  cinvcode AS sku, 
  threecategory_name_exhibition, 
  main_project_group_name, 
  status, 
  cinvproductstatus, 
  category_attribute
  FROM bi.sku_detail_dim
  ) pur ON TRIM(rg.sku) = TRIM(pur.sku)
"""

# 缓存装饰器
@st.cache_data(ttl=3600)  # 缓存1小时
def get_data_from_db():
    """
    从数据库获取数据
    返回：DataFrame或None
    """
    try:
        # 建立数据库连接
        st.info("正在连接数据库...")
        start_time = time.time()
        connection = mysql.connector.connect(**db_config)
        
        if connection.is_connected():
            st.success("数据库连接成功！")
            
            # 执行查询
            st.info("正在执行查询...")
            df = pd.read_sql(query, connection)
            st.success(f"数据获取成功，共 {len(df)} 条记录，耗时 {time.time() - start_time:.2f} 秒")
            return df
    
    except Error as e:
        st.error(f"数据库连接或查询错误: {e}")
        st.warning("尝试使用备用数据...")
        return get_sample_data()
    
    finally:
        # 关闭连接
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            st.info("数据库连接已关闭")

def get_sample_data():
    """
    获取示例数据作为备用
    """
    # 创建示例数据
    data = {
        "产品编码": [f"P{i:04d}" for i in range(1, 101)],
        "主项目组": [f"项目组{i % 5 + 1}" for i in range(100)],
        "产品销售状态": ["正常" if i % 2 == 0 else "暂停" for i in range(100)],
        "产品状态": ["活跃" if i % 3 == 0 else "非活跃" for i in range(100)],
        "末级品类": [f"品类{i % 8 + 1}" for i in range(100)],
        "产品属性名称": ["属性A" if i % 2 == 0 else "属性B" for i in range(100)]
    }
    df = pd.DataFrame(data)
    st.info(f"使用示例数据，共 {len(df)} 条记录")
    return df
# 缓存装饰器
@st.cache_data(ttl=3600)  # 缓存1小时
def process_data(df):
    """
    处理数据，去除空值和重复值
    """
    try:
        # 去除空值
        df_clean = df.dropna()
        st.info(f"数据清洗后，共 {len(df_clean)} 条记录")
        
        # 去除重复的产品编码
        df_unique = df_clean.drop_duplicates(subset=["产品编码"])
        st.info(f"去重后，共 {len(df_unique)} 个唯一产品编码")
        
        return df_unique
    
    except Exception as e:
        st.error(f"数据处理错误: {e}")
        return None
def create_bar_chart(df):
    """
    创建主项目组产品数量条形图
    """
    try:
        # 按主项目组分组，计算去重产品编码数量
        project_group_count = df.groupby("主项目组").size().reset_index(name="产品数量")
        project_group_count = project_group_count.sort_values(by="产品数量", ascending=False)
        # 创建条形图
        fig = px.bar(
            project_group_count, 
            x="主项目组", 
            y="产品数量", 
            title="各主项目组产品数量",
            color_discrete_sequence=["#1f77b4"],  # 蓝色调
            text="产品数量"
        )
        
        # 美化图表
        fig.update_layout(
            xaxis_title="主项目组",
            yaxis_title="产品数量",
            template="plotly_white",
            height=500
        )
        
        return fig
    
    except Exception as e:
        st.error(f"创建条形图错误: {e}")
        return None

def create_pie_chart(df):
    """
    创建末级品类产品数量占比饼图
    """
    try:
        # 按末级品类分组，计算去重产品编码数量
        category_count = df.groupby("末级品类").size().reset_index(name="产品数量")
        category_count = category_count.sort_values(by="产品数量", ascending=False)
        
        # 处理品类数量过多的情况，只显示前10个，其余归为"其他"
        if len(category_count) > 10:
            top_categories = category_count.head(10)
            other_count = category_count.tail(len(category_count) - 10)["产品数量"].sum()
            other_row = pd.DataFrame([["其他", other_count]], columns=["末级品类", "产品数量"])
            category_count = pd.concat([top_categories, other_row], ignore_index=True)
            
        # 创建饼图
        fig = px.pie(
            category_count, 
            names="末级品类", 
            values="产品数量", 
            title="末级品类产品数量占比",
            color_discrete_sequence=px.colors.sequential.Blues  # 蓝色调
        )
        
        # 美化图表
        fig.update_layout(
            template="plotly_white",
            height=500
        )
        
        return fig
    
    except Exception as e:
        st.error(f"创建饼图错误: {e}")
        return None

def main():
    """
    主函数
    """
    # 设置页面标题和布局
    st.set_page_config(
        page_title="产品分析看板",
        page_icon="📊",
        layout="wide"
    )
    # 页面标题
    st.title("2025年上半年销售数据分析看板")
    st.markdown("""
    本看板展示了2025年上半年的销售数据，包括产品数量、销售状态、项目组分布等。
    您可以使用侧边栏的筛选器来定制分析范围。
    """)
    # 数据获取和处理
    df = get_data_from_db()
    
    if df is not None:
        df_processed = process_data(df)
        
        if df_processed is not None and len(df_processed) > 0:
            # 侧边栏筛选器
            st.sidebar.title("筛选器")
            
            # 主项目组筛选
            project_groups = df_processed["主项目组"].unique()
            selected_project_groups = st.sidebar.multiselect(
                "选择主项目组",
                options=project_groups,
                default=project_groups[:5] if len(project_groups) > 5 else project_groups
            )
            
            # 末级品类筛选
            categories = df_processed["末级品类"].unique()
            selected_categories = st.sidebar.multiselect(
                "选择末级品类",
                options=categories,
                default=categories[:5] if len(categories) > 5 else categories
            )
            
            # 产品销售状态筛选
            sale_status = df_processed["产品销售状态"].unique()
            selected_sale_status = st.sidebar.multiselect(
                "选择产品销售状态",
                options=sale_status,
                default=sale_status
            )
            
            # 产品状态筛选
            product_status = df_processed["产品状态"].unique()
            selected_product_status = st.sidebar.multiselect(
                "选择产品状态",
                options=product_status,
                default=product_status
            )
            
            # 应用筛选
            filtered_df = df_processed[
                (df_processed["主项目组"].isin(selected_project_groups)) &
                (df_processed["末级品类"].isin(selected_categories)) &
                (df_processed["产品销售状态"].isin(selected_sale_status)) &
                (df_processed["产品状态"].isin(selected_product_status))
            ]
            
            st.info(f"筛选后，共 {len(filtered_df)} 个产品")
            
            # 显示数据表格
            st.subheader("数据概览")
            st.dataframe(filtered_df.head(20))  # 只显示前20行
            
            # 创建图表
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("各主项目组产品数量")
                bar_fig = create_bar_chart(filtered_df)
                if bar_fig:
                    st.plotly_chart(bar_fig, use_container_width=True)
            
            with col2:
                st.subheader("末级品类产品数量占比")
                pie_fig = create_pie_chart(filtered_df)
                if pie_fig:
                    st.plotly_chart(pie_fig, use_container_width=True)
            
            # 数据下载
            st.subheader("数据下载")
            csv = df_processed.to_csv(index=False)
            st.download_button(
                label="下载完整数据 (CSV)",
                data=csv,
                file_name="product_analysis_data.csv",
                mime="text/csv"
            )
        else:
            st.warning("处理后的数据为空，请检查数据质量")

    else:
        st.warning("未能获取数据，请检查数据库连接和查询")

if __name__ == "__main__":
    main()
