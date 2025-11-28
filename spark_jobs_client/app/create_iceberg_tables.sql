CREATE TABLE IF NOT EXISTS iceberg_mysql_catalog.total_orders_db.total_orders (
    ID                                                  STRING NOT NULL COMMENT 'UUID 无业务属性',
    Order_ID                                            STRING NOT NULL COMMENT '系统单号',
    Reference_ID                                        STRING COMMENT '参考号',
    Order_Status                                        STRING NOT NULL COMMENT '状态',
    Item_Status                                         STRING COMMENT '配送状态',
    Last_Update_Datetime                                TIMESTAMP COMMENT '上次更新日期', -- DATETIME 转换为 TIMESTAMP
    Ship_Service_Level                                  STRING COMMENT '发货服务级别',
    Fulfillment_Channel                                 STRING COMMENT '平台 履行渠道',
    Store                                               STRING NOT NULL COMMENT '店铺',
    Sales_Channel                                       STRING COMMENT '站点 销售渠道',
    Is_Business_Order                                   BOOLEAN COMMENT '是否企业订单', -- BOOLEAN DEFAULT FALSE 转换为 BOOLEAN
    Purchase_Order_Number                               STRING COMMENT '采购订单号',
    Note                                                STRING COMMENT '客服备注',
    Remark_From_Buyer                                   STRING COMMENT '买家备注',
    Order_Source                                        STRING COMMENT '订单来源',
    Shipment_Method                                     STRING COMMENT '订单类型 发货方式',
    Tags                                                STRING COMMENT '标签 Parent SKU Reference No',
    Purchase_Datetime                                   TIMESTAMP NOT NULL COMMENT '订购时间', -- DATETIME 转换为 TIMESTAMP
    Payment_Datetime                                    TIMESTAMP COMMENT '付款时间', -- DATETIME 转换为 TIMESTAMP
    Ship_Deadline_Datetime                              TIMESTAMP COMMENT '发货时限', -- DATETIME 转换为 TIMESTAMP
    Ship_Datetime                                       TIMESTAMP COMMENT '发货时间', -- DATETIME 转换为 TIMESTAMP
    Estimated_Ship_Out_Datetime                         TIMESTAMP COMMENT '标发时间', -- DATETIME 转换为 TIMESTAMP
    Do_Not_Ship_Datetime                                TIMESTAMP COMMENT '不发货时间', -- DATETIME 转换为 TIMESTAMP
    Scheduled_Delivery_Datetime                         TIMESTAMP COMMENT '指定配送时间', -- DATETIME 转换为 TIMESTAMP
    Order_Complete_Datetime                             TIMESTAMP COMMENT '订单完成时间', -- DATETIME 转换为 TIMESTAMP
    Product_Name                                        STRING NOT NULL COMMENT '品名',
    MSKU                                                STRING COMMENT 'MSKU',
    Product_ID                                          STRING NOT NULL COMMENT '商品Id',
    Price_Designation                                   STRING COMMENT '价格标识',
    Signature_Confirmation_Recommended                  BOOLEAN COMMENT '建议签名确认', -- BOOLEAN DEFAULT FALSE 转换为 BOOLEAN
    Order_Item_ID                                       STRING COMMENT '订单商品ID',
    Product_Title                                       STRING COMMENT '商品标题 也是Product_Name',
    _69_Code                                            STRING COMMENT '69码', -- 反引号 ` 是为了处理列名以数字开头或包含特殊字符的情况
    Variation_Name                                      STRING NOT NULL COMMENT '规格名称',
    Variation_Attribute                                 STRING COMMENT '变体属性',
    Original_Price                                      DECIMAL(10, 2) COMMENT '原始价格', -- 移除 DEFAULT 0.00
    Deal_Price                                          DECIMAL(10, 2) COMMENT '成交价格', -- 移除 DEFAULT 0.00
    Unit_Price                                          DECIMAL(10, 2) COMMENT '单价', -- 移除 DEFAULT 0.00
    Quantity                                            INT COMMENT '数量', -- 移除 DEFAULT 0
    Number_Of_Pieces                                    INT COMMENT '件数', -- 移除 DEFAULT 0
    Return_Quantity                                     INT COMMENT '退货数量', -- 移除 DEFAULT 0
    Item_Price                                          DECIMAL(10, 2) COMMENT '商品金额', -- 移除 DEFAULT 0.00
    Service_Fee                                         DECIMAL(10, 2) COMMENT '服务费', -- 移除 DEFAULT 0.00
    Products_Price_Paid_By_Buyer                        DECIMAL(10, 2) COMMENT '实付金额', -- 移除 DEFAULT 0.00
    Credit_Card_Discount_Total                          DECIMAL(10, 2) COMMENT '信用卡折扣', -- 移除 DEFAULT 0.00
    Coin_Offset                                         DECIMAL(10, 2) COMMENT '金币抵扣', -- 移除 DEFAULT 0.00
    Reverse_Shipping_Fee                                DECIMAL(10, 2) COMMENT '退货运费', -- 移除 DEFAULT 0.00
    Shipping_Price                                      DECIMAL(10, 2) COMMENT '商品客付运费', -- 移除 DEFAULT 0.00
    Item_Tax                                            DECIMAL(10, 2) COMMENT '商品客付税费', -- 移除 DEFAULT 0.00
    Shipping_Tax                                        DECIMAL(10, 2) COMMENT '运费税', -- 移除 DEFAULT 0.00
    Gift_Wrap_Price                                     DECIMAL(10, 2) COMMENT '礼品包装价格', -- 移除 DEFAULT 0.00
    Gift_Wrap_Tax                                       DECIMAL(10, 2) COMMENT '礼品包装税', -- 移除 DEFAULT 0.00
    Product_Tip                                         DECIMAL(10, 2) COMMENT '商品小费', -- 移除 DEFAULT 0.00
    Item_Promotion_Discount                             DECIMAL(10, 2) COMMENT '商品折扣', -- 移除 DEFAULT 0.00
    Store_Discount                                      DECIMAL(10, 2) COMMENT '店铺折扣', -- 移除 DEFAULT 0.00
    Store_Voucher                                       DECIMAL(10, 2) COMMENT '店铺优惠券', -- 移除 DEFAULT 0.00
    Platform_Voucher                                    DECIMAL(10, 2) COMMENT '平台优惠券', -- 移除 DEFAULT 0.00
    Platform_Discount                                   DECIMAL(10, 2) COMMENT '平台折扣', -- 移除 DEFAULT 0.00
    Ship_Promotion_Discount                             DECIMAL(10, 2) COMMENT '运费折扣', -- 移除 DEFAULT 0.00
    Transaction_Fee                                     DECIMAL(10, 2) COMMENT '商品交易费', -- 移除 DEFAULT 0.00
    Promotion_Ids                                       STRING COMMENT '促销代码',
    Product_Platform_Subsidy                            DECIMAL(10, 2) COMMENT '商品平台补贴', -- 移除 DEFAULT 0.00
    Product_Platform_Other_Fees                         DECIMAL(10, 2) COMMENT '商品平台其它费', -- 移除 DEFAULT 0.00
    Product_Outbound_Cost                               DECIMAL(10, 2) COMMENT '商品出库成本', -- 移除 DEFAULT 0.00
    Product_Shipping_Cost_Currency                      STRING COMMENT '商品物流运费币种',
    Product_Shipping_Cost                               DECIMAL(10, 2) COMMENT '商品物流运费', -- 移除 DEFAULT 0.00
    Product_Notes                                       STRING COMMENT '商品备注',
    Product_Manager                                     STRING COMMENT '商品负责人',
    Product_Tags                                        STRING COMMENT '商品标签',
    Buyer_Paid_Shipping_Fee                             DECIMAL(10, 2) COMMENT '买家支付运费', -- 移除 DEFAULT 0.00
    Platform_Bundle_Discount                            DECIMAL(10, 2) COMMENT '平台组合折扣', -- 移除 DEFAULT 0.00
    Seller_Bundle_Discount                              DECIMAL(10, 2) COMMENT '卖家组合折扣', -- 移除 DEFAULT 0.00
    Earliest_Label_Acquisition_Datetime                 TIMESTAMP COMMENT '最早获取面单时间', -- DATETIME 转换为 TIMESTAMP
    Customer_Selected_Logistics                         STRING COMMENT '客选物流',
    Shipping_Warehouse                                  STRING COMMENT '发货仓库',
    Logistics_Method                                    STRING COMMENT '物流方式',
    Tracking_Number                                     STRING COMMENT '运单号',
    Tracking_Number_External                            STRING COMMENT '跟踪号',
    Labeled_Number                                      STRING COMMENT '标发号',
    Tax_ID_Type                                         STRING COMMENT '税号类型',
    Sender_Tax_ID                                       STRING COMMENT '寄件税号',
    COD_Order                                           STRING COMMENT 'COD订单',
    Estimated_Weight_g                                  DECIMAL(10, 3) COMMENT '预估重量(g)', -- 移除 DEFAULT 0.000
    Estimated_Length_cm                                 DECIMAL(10, 3) COMMENT '预估尺寸长(cm)', -- 移除 DEFAULT 0.000
    Estimated_Width_cm                                  DECIMAL(10, 3) COMMENT '预估尺寸宽(cm)', -- 移除 DEFAULT 0.000
    Estimated_Height_cm                                 DECIMAL(10, 3) COMMENT '预估尺寸高(cm)', -- 移除 DEFAULT 0.000
    Estimated_Billable_Weight_g                         DECIMAL(10, 3) COMMENT '预估计费重(g)', -- 移除 DEFAULT 0.000
    Estimated_Shipping_Cost_CNY                         DECIMAL(10, 3) COMMENT '预估运费(CNY)', -- 移除 DEFAULT 0.000
    SKU_Total_Weight                                    DECIMAL(10, 3) COMMENT 'sku重量', -- 移除 DEFAULT 0.000
    Order_Total_Weight_g                                DECIMAL(10, 3) COMMENT '包裹实重(g)', -- 移除 DEFAULT 0.000
    Package_Length_cm                                   DECIMAL(10, 3) COMMENT '包裹尺寸长(cm)', -- 移除 DEFAULT 0.000
    Package_Width_cm                                    DECIMAL(10, 3) COMMENT '包裹尺寸宽(cm)', -- 移除 DEFAULT 0.000
    Package_Height_cm                                   DECIMAL(10, 3) COMMENT '包裹尺寸高(cm)', -- 移除 DEFAULT 0.000
    Actual_Billable_Weight_g                            DECIMAL(10, 3) COMMENT '实际计费重(g)', -- 移除 DEFAULT 0.000
    Shipping_Cost_Currency                              STRING COMMENT '物流运费币种',
    Shipping_Cost                                       DECIMAL(10, 2) COMMENT '物流运费', -- 移除 DEFAULT 0.00
    Currency                                            STRING COMMENT '订单币种',
    Total_Amount                                        DECIMAL(12, 2) COMMENT '订单总金额', -- 移除 DEFAULT 0.00
    Outbound_Cost_CNY                                   DECIMAL(10, 2) COMMENT '订单出库成本(CNY)', -- 移除 DEFAULT 0.00
    Estimated_Gross_Profit                              DECIMAL(10, 2) COMMENT '预估毛利润', -- 移除 DEFAULT 0.00
    Estimated_Gross_Margin                              DECIMAL(5, 4) COMMENT '预估毛利率', -- 移除 DEFAULT 0.0000
    Buyer_Name                                          STRING COMMENT '买家姓名',
    Buyer_Email                                         STRING COMMENT '买家邮箱',
    Buyer_Message                                       STRING COMMENT '买家留言',
    Recipient                                           STRING COMMENT '收件人',
    Phone                                               STRING COMMENT '电话',
    Ship_Country                                        STRING COMMENT '国家/地区',
    Ship_State                                          STRING COMMENT '省/州',
    Ship_City                                           STRING COMMENT '城市',
    District                                            STRING COMMENT '区/县',
    Town                                                STRING COMMENT '城镇',
    Zip_Code                                            STRING COMMENT '邮编',
    House_Number                                        STRING COMMENT '门牌号',
    Cancel_Reason                                       STRING COMMENT '订单取消原因',
    Return_Refund_Status                                STRING COMMENT '退货/退款状态',
    Address_Type                                        STRING COMMENT '地址类型',
    Company_Name                                        STRING COMMENT '公司名',
    Address_Line_1                                      STRING COMMENT '地址行1',
    Address_Line_2                                      STRING COMMENT '地址行2',
    Address_Line_3                                      STRING COMMENT '地址行3',
    Username_Buyer                                      STRING COMMENT '用户名 买家',
    Payment_Method                                      STRING COMMENT '付款方式',
    Payment_Method_Details                              STRING COMMENT '付款方式 详情',
    Installment_Plan                                    STRING COMMENT '分期付款计划',
    Handling_Fee                                        DECIMAL(10, 2) COMMENT '手续费', 
    Bundle_Deals_Indicator                              BOOLEAN COMMENT '商品备注？？？？(Y/N)', -- BOOLEAN DEFAULT FALSE 转换为 BOOLEAN
    Order_Cancel_Datetime                               TIMESTAMP COMMENT '订单取消日期', -- DATETIME 转换为 TIMESTAMP
    Commission_Fee                                      DECIMAL(10, 2) COMMENT '佣金', -- 移除 DEFAULT 0.00
    Package_ID                                          STRING COMMENT '货物编号',
    Is_Best_Selling_Product                             BOOLEAN COMMENT '是否热销产品', -- BOOLEAN DEFAULT FALSE 转换为 BOOLEAN
    Weight_kg                                           DECIMAL(10, 3) COMMENT '重量(kg)', -- 移除 DEFAULT 0.000
    Region                                              STRING COMMENT '大区',
    Product_Category                                    STRING COMMENT '产品类目',
    Session                                             INT COMMENT '访客', -- 移除 DEFAULT 0
    Product_Cost                                        DECIMAL(10, 2) COMMENT '产品成本', -- 移除 DEFAULT 0.00
    Net_Profit                                          DECIMAL(10, 2) COMMENT '销售净利', -- 移除 DEFAULT 0.00
    Net_Margin_Rate                                     DECIMAL(5, 4) COMMENT '净利率', -- 移除 DEFAULT 0.00
    Rebate                                              DECIMAL(10, 2) COMMENT '返利', -- 移除 DEFAULT 0.00
    First_Leg_Shipping_Cost                             DECIMAL(10, 2) COMMENT '头程', -- 移除 DEFAULT 0.00
    Affiliate_Marketing                                 STRING COMMENT '联盟营销',
    Return_Rate                                         DECIMAL(5, 4) COMMENT '退货率', -- 移除 DEFAULT 0.00
    Return_Fee                                          DECIMAL(10, 2) COMMENT '退货费用', -- 移除 DEFAULT 0.00
    Advertising_Spend                                   DECIMAL(10, 2) COMMENT '广告花费', -- 移除 DEFAULT 0.00
    Shipping_Rebate_Estimate                            DECIMAL(10, 2) COMMENT '运费回扣估算'
)
USING iceberg
TBLPROPERTIES (
    'write.format.default'='parquet',
    'comment'='订单总表' -- 表级注释
);

