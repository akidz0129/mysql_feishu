USE test;

create table if not exists total_orders(
    ID                                                  VARCHAR(36) NOT NULL PRIMARY KEY    COMMENT 'UUID 无业务属性'                      ,
    Purchase_Datetime                                   DATETIME NOT NULL                   COMMENT '订购时间'                ,                   -- (订购时间)
    Order_ID                                            VARCHAR(50) NOT NULL                COMMENT '系统单号'                          ,                   -- (系统单号)
    Reference_ID                                        VARCHAR(50)                         COMMENT '参考号'                            ,                   -- (参考号)
    Region                                              VARCHAR(50)                         COMMENT '大区'                        ,         -- 推荐：对应“大区”，如果需要单独存储区域信息    
    Ship_Country_Name                                   VARCHAR(20)                         COMMENT '国家中文名'                ,                   -- (国家/地区)
    Ship_Country                                        CHAR(2)                             COMMENT '国家'                ,                   -- (国家/地区)
    Currency                                            CHAR(3)                             COMMENT '订单币种'                ,                   -- (订单币种)
    Exchange_Rate                                       DECIMAL(10, 6)              COMMENT '汇率', 
    Fulfillment_Channel                                 VARCHAR(20)                 COMMENT '平台 履行渠道'                     ,                   -- (平台)履行渠道
    Store                                               VARCHAR(255) NOT NULL       COMMENT '店铺'                              ,                   -- (店铺)表格没有 手动添加
    Tags                                                VARCHAR(255)                COMMENT '父ASIN/spuSPU标签 Parent SKU Reference No'         ,                   -- (标签)Parent SKU Reference No
    Product_ID                                          VARCHAR(255) NOT NULL       COMMENT '子ASIN/sku商品Id'                  ,                   -- (商品Id)
    MSKU                                                VARCHAR(255)                COMMENT 'MSKU'                    ,                   -- (MSKU)
    MSKU_True                                           VARCHAR(50)                 COMMENT'MSKU_True',
    Item_ID                                             VARCHAR(50)                 COMMENT'商品ID',
    GTIN                                                VARCHAR(50)                 COMMENT'GTIN',
    Product_Chinese_Name                                VARCHAR(255)                COMMENT '中文品名',
    Product_Name                                        VARCHAR(255)                COMMENT '品名',           
    Variation_Name                                      VARCHAR(255)                COMMENT '规格',          
    Quantity                                            INT                         COMMENT '销量',    
    Item_Price                                          DECIMAL(10, 2)              COMMENT '商品金额', 
    Buyer_Paid_Amount                                   DECIMAL(10, 2)              COMMENT '买家实付金额',
    Sale_Amount                                         DECIMAL(10, 2)              COMMENT '实际销售额',
    Repayment_Amount                                    DECIMAL(10, 2)              COMMENT '手动计算回款额',
    Profit                                              DECIMAL(10, 2)              COMMENT '最终利润',
    Seller_Voucher                                      DECIMAL(10, 2)              COMMENT '卖家优惠券金额', 
    Platform_Voucher                                    DECIMAL(10, 2)              COMMENT '平台优惠券', 
    Coin_Offset                                         DECIMAL(10, 2)              COMMENT '币抵扣',
    Buyer_Paid_Shipping_Fee                             DECIMAL(10, 2)              COMMENT '买家支付运费', 
    Seller_Paid_Shipping_Fee                            DECIMAL(10, 2)              COMMENT '卖家支付运费' ,
    Actual_Shipping_Fee                                 DECIMAL(10, 2)              COMMENT '实际物流运费', 
    Shipping_Subsidy                                    DECIMAL(10, 2)              COMMENT '运费返还', 
    Platform_Discount                                   DECIMAL(10, 2)              COMMENT '平台回扣',
    Initial_Buyer_TXN_Fee                               DECIMAL(10, 2)              COMMENT '买家初始交易费',
    Buyer_Service_Fee                                   DECIMAL(10, 2)              COMMENT '买家服务费',
    Insurance_Premium                                   DECIMAL(10, 2)              COMMENT '保险费',
    Shipping_Fee_SST_Amount                             DECIMAL(10, 2)              COMMENT '运费SST',
    Item_Tax                                            DECIMAL(10, 2)              COMMENT '商品客付税费'             ,                   -- (商品客付税费)
    Affiliate_Marketing                                 DECIMAL(10, 2)              COMMENT '联盟营销方案佣金', 
    Commission_Fee                                      DECIMAL(10, 2)              COMMENT '佣金'                      ,                   -- 佣金
    Service_Fee                                         DECIMAL(10, 2)              COMMENT '服务费'                  ,                   -- (服务费)
    Transaction_Fee                                     DECIMAL(10, 2)              COMMENT '商品交易费'               ,                   -- (商品交易费)
    Delivery_Seller_Protection_Fee_Premium_Amount       DECIMAL(10, 2)              COMMENT '配送卖家保护费高级金额',
    Seller_Coin_Cash_Back                               DECIMAL(10, 2)              COMMENT '卖家币返还',
    Seller_Order_Processing_Fee                         DECIMAL(10, 2)              COMMENT '订单处理费',
    Vat                                                 DECIMAL(10, 2)              COMMENT '增值税',
    Return_Quantity                                     INT                         COMMENT '退货数量',
    Return_Price                                        DECIMAL(10, 2)              COMMENT '退款金额',
    Return_Credit                                       DECIMAL(10, 2)              COMMENT '退款入账',
    Return_Debit                                        DECIMAL(10, 2)              COMMENT '退款支出',
    Product_Cost                                        DECIMAL(10, 2)              COMMENT '产品成本'                    ,         --     产品成本: "" # 无直接对应
    First_Leg_Shipping_Cost                             DECIMAL(10, 2)              COMMENT '头程',
    Outbound_Cost                                       DECIMAL(10, 2)              COMMENT '出库费'                    ,                   -- (订单出库成本
    Shipping_Cost                                       DECIMAL(10, 2)              COMMENT '运费非平台'                ,                   -- (物流运费)
    Advertising_Spend                                   DECIMAL(10, 2)              COMMENT '广告花费'                    ,         -- : "" # 无直接对应
    Lost_Claim                                          DECIMAL(10, 2)              COMMENT '丢件索赔',
    Other_Debit                                         DECIMAL(10, 2)              COMMENT '其它支出',
    Influencer_Commission_Fee                           DECIMAL(10, 2)              COMMENT '达人佣金'                            ,
    Influencer_Partner_Commission_Fee                   DECIMAL(10, 2)              COMMENT '达人合作伙伴佣金'                    ,
    SFP_Service_Fee                                     DECIMAL(10, 2)              COMMENT 'SFP服务费'                           ,
    Bonus_Cashback_Service_Fee                          DECIMAL(10, 2)              COMMENT 'Bonus Cashback Service Fee'          ,
    Profit_Adjustment                                   DECIMAL(10, 2)              COMMENT '补毛保',
    Invoice_Fee                                         DECIMAL(10, 2)              COMMENT '开票费用',
    Amortization                                        DECIMAL(10, 2)              COMMENT '摊销',
    Ship_Promotion_Discount                             DECIMAL(10, 2)              COMMENT '促销费-运费折扣'                ,                   -- (促销费-运费折扣)
    Item_Promotion_Discount                             DECIMAL(10, 2)              COMMENT '促销费-商品折扣'                ,                   -- '促销费-商品折扣'
    FBA_Fee                                             DECIMAL(10, 2)              COMMENT 'FBA费',
    Promotion_Discount                                  DECIMAL(10, 2)              COMMENT '促销折扣',
    Inventory_Reimbursement                             DECIMAL(10, 2)              COMMENT '库存赔偿',
    Other_Credit                                        DECIMAL(10, 2)              COMMENT '其他收入',
    Fee_Refund_Amount                                   DECIMAL(10, 2)              COMMENT '费用退款额',
    Promotion_Cost                                      DECIMAL(10, 2)              COMMENT '推广费',
    Storage_Fee                                         DECIMAL(10, 2)              COMMENT '仓储费',
    Inbound_Cost                                        DECIMAL(10, 2)              COMMENT '入库配置费',
    Adjustment_Cost                                     DECIMAL(10, 2)              COMMENT '调整费用',
    Removal_Cost                                        DECIMAL(10, 2)              COMMENT '移除费',
    Transaction_Service_Fee                             DECIMAL(10, 2)              COMMENT '交易服务费',
    JD_Paid_Global_Sale_Tax                             DECIMAL(10, 2)              COMMENT '京东代付全球售消费税',
    JD_Collected_Global_Sale_Tax                        DECIMAL(10, 2)              COMMENT '京东代收全球售消费税',
    JD_Points                                           DECIMAL(10, 2)              COMMENT '京豆',
    Collected_Shipping_Fee                              DECIMAL(10, 2)              COMMENT '代收配送费',
    Price_Protection_Deduction                          DECIMAL(10, 2)              COMMENT '价保扣款',
    Price_Protection_Rebate                             DECIMAL(10, 2)              COMMENT '价保返佣',
    Seller_Shipping_Return                              DECIMAL(10, 2)              COMMENT '卖家返还运费',
    After_Sale_Seller_Compensation                      DECIMAL(10, 2)              COMMENT '售后卖家赔付费',
    Product_Insurance_Fee                               DECIMAL(10, 2)              COMMENT '商品保险服务费',
    Direct_Compensation_Fee                             DECIMAL(10, 2)              COMMENT '商家直赔费',
    Small_Payment_Refund                                DECIMAL(10, 2)              COMMENT '小额打款费用退还',
    Platform_Coupon_Subsidy                             DECIMAL(10, 2)              COMMENT '平台券价保补贴',
    Platform_Coupon_Subsidy_Commission                  DECIMAL(10, 2)              COMMENT '平台券价保补贴佣金',
    Ad_Cooperation_Deduction_Commission                 DECIMAL(10, 2)              COMMENT '广告联合活动降扣佣金',
    Comprehensive_Penalty                               DECIMAL(10, 2)              COMMENT '综合违约金',
    Shipping_Insurance_Fee                              DECIMAL(10, 2)              COMMENT '运费保险服务费',
    SOP_Double_Compensation_JDPoints                    DECIMAL(10, 2)              COMMENT 'SOP双倍赔款（京豆赔付）',
    SOP_Double_Compensation_Commission                  DECIMAL(10, 2)              COMMENT 'SOP双倍赔返还佣金',
    JD_Promotion_Tech_Service_Fee                       DECIMAL(10, 2)              COMMENT '代收白条网络推广技术服务费',
    NonPlatform_National_Subsidy_Transaction_Fee        DECIMAL(10, 2)              COMMENT '非平台核销国补交易服务费',
    NonPlatform_National_Subsidy_Commission             DECIMAL(10, 2)              COMMENT '非平台核销国补佣金',
    NonPlatform_National_Subsidy_Ad_Commission          DECIMAL(10, 2)              COMMENT '非平台核销国补广告佣金',
    CONSTRAINT UX_ UNIQUE(Order_ID, Product_ID)                
        )COMMENT='订单总表';
    
create table if not exists shopee_table(

        Purchase_Datetime                               DATETIME                        COMMENT '订购日期',  
        Order_ID                                        VARCHAR(50) NOT NULL            COMMENT '订单号',  
        Region                                          VARCHAR(20)                     COMMENT '大区',
        Ship_Country_Name                               VARCHAR(255)                    COMMENT '国家名',       
        Ship_Country                                    CHAR(2)                         COMMENT '国家', 
        Currency                                        CHAR(3)                         COMMENT '原币种',
        Exchange_Rate                                   DECIMAL(10, 6)                  COMMENT '汇率', 
        Fulfillment_Channel                             VARCHAR(20)                     COMMENT '渠道',                      
        Store                                           VARCHAR(50)                     COMMENT '店铺',                 
        Tags                                            VARCHAR(50)                     COMMENT '父ASIN/spu',         
        Product_ID                                      VARCHAR(50) NOT NULL            COMMENT '子ASIN/sku',         
        MSKU                                            VARCHAR(50)                     COMMENT 'MSKU',             
        Product_Chinese_Name                            VARCHAR(255)                    COMMENT '中文品名',
        Product_Name                                    VARCHAR(255)                    COMMENT '品名',           
        Variation_Name                                  VARCHAR(255)                    COMMENT '规格',          
        Quantity                                        INT                             COMMENT '销量',    
        Item_Price                                      DECIMAL(10, 2)                  COMMENT '商品金额', 
        Seller_Voucher                                  DECIMAL(10, 2)                  COMMENT '卖家优惠券金额', 
        Platform_Voucher                                DECIMAL(10, 2)                  COMMENT '平台优惠券', 
        Coin_Offset                                     DECIMAL(10, 2)                  COMMENT '币抵扣',
        Buyer_Paid_Shipping_Fee                         DECIMAL(10, 2)                  COMMENT '买家支付运费', 
        Actual_Shipping_Fee                             DECIMAL(10, 2)                  COMMENT '实际物流运费', 
        Shipping_Subsidy                                DECIMAL(10, 2)                  COMMENT '运费返还', 
        Platform_Discount                               DECIMAL(10, 2)                  COMMENT '平台回扣',
        Initial_Buyer_TXN_Fee                           DECIMAL(10, 2)                  COMMENT '买家初始交易费',
        Buyer_Service_Fee                               DECIMAL(10, 2)                  COMMENT '买家服务费',
        Insurance_Premium                               DECIMAL(10, 2)                  COMMENT '保险费',
        Shipping_Fee_SST_Amount                         DECIMAL(10, 2)                  COMMENT '运费SST',
        Seller_Order_Processing_Fee                     DECIMAL(10, 2)                  COMMENT '订单处理费',
        Affiliate_Marketing                             DECIMAL(10, 2)                  COMMENT '联盟营销方案佣金', 
        Commission_Fee                                  DECIMAL(10, 2)                  COMMENT '佣金',
        Service_Fee                                     DECIMAL(10, 2)                  COMMENT '服务费', 
        Transaction_Fee                                 DECIMAL(10, 2)                  COMMENT '交易费',
        Delivery_Seller_Protection_Fee_Premium_Amount   DECIMAL(10, 2)                  COMMENT '配送卖家保护费高级金额',
        Seller_Coin_Cash_Back                           DECIMAL(10, 2)                  COMMENT '卖家币返还',
        Vat                                             DECIMAL(10, 2)                  COMMENT '增值税',
        Return_Quantity                                 INT                             COMMENT '退货数量',
        Return_Price                                    DECIMAL(10, 2)                  COMMENT '退款金额',
        Return_Seller_Voucher                           DECIMAL(10, 2)                  COMMENT '退还卖家优惠券金额',                                              
        Return_Shipping_Cost                            DECIMAL(10, 2)                  COMMENT '退货运费',                      
        Product_Cost                                    DECIMAL(10, 2)                  COMMENT '产品成本',  
        First_Leg_Shipping_Cost                         DECIMAL(10, 2)                  COMMENT '头程',
        Outbound_Cost                                   DECIMAL(10, 2)                  COMMENT '出库费',                              
        Shipping_Cost                                   DECIMAL(10, 2)                  COMMENT '运费',      
        Advertising_Spend                               DECIMAL(10, 2)                  COMMENT '广告花费',
        PRIMARY KEY (Order_ID, Product_ID)

) COMMENT='shopee总表';

create table if not exists shopee_data(
        Purchase_Datetime                               DATETIME                        COMMENT '订购日期',  
        Ship_Country                                    CHAR(2)                         COMMENT '国家', 
        Currency                                        CHAR(3)                         COMMENT '原币种',
        Fulfillment_Channel                             VARCHAR(20)                     COMMENT '渠道',                      
        Store                                           VARCHAR(50)                     COMMENT '店铺',                 
        Ship_Country_Name                               VARCHAR(255)                    COMMENT '国家名',       
        Order_ID                                        VARCHAR(50) NOT NULL            COMMENT '订单号',  
        MSKU                                            VARCHAR(50)                     COMMENT 'MSKU',             
        Tags                                            VARCHAR(50)                     COMMENT '父ASIN/spu',         
        Product_ID                                      VARCHAR(50) NOT NULL            COMMENT '子ASIN/sku',         
        Product_Name                                    VARCHAR(255)                    COMMENT '品名',           
        Variation_Name                                  VARCHAR(255)                    COMMENT '规格',          
        Quantity                                        INT                             COMMENT '销量',    
        Item_Price                                      DECIMAL(10, 2)                  COMMENT '商品金额', 
        Seller_Voucher                                  DECIMAL(10, 2)                  COMMENT '卖家优惠券金额', 
        Platform_Voucher                                DECIMAL(10, 2)                  COMMENT '平台优惠券', 
        Initial_Buyer_TXN_Fee                           DECIMAL(10, 2)                  COMMENT '买家初始交易费',
        Insurance_Premium                               DECIMAL(10, 2)                  COMMENT '保险费',
        Coin_Offset                                     DECIMAL(10, 2)                  COMMENT '币抵扣',
        Buyer_Service_Fee                               DECIMAL(10, 2)                  COMMENT '买家服务费',
        Shipping_Fee_SST_Amount                         DECIMAL(10, 2)                  COMMENT '运费SST',
        Buyer_Paid_Shipping_Fee                         DECIMAL(10, 2)                  COMMENT '买家支付运费', 
        Actual_Shipping_Fee                             DECIMAL(10, 2)                  COMMENT '实际物流运费', 
        Shipping_Subsidy                                DECIMAL(10, 2)                  COMMENT '运费返还', 
        Affiliate_Marketing                             DECIMAL(10, 2)                  COMMENT '联盟营销方案佣金', 
        Platform_Discount                               DECIMAL(10, 2)                  COMMENT '平台回扣',
        Seller_Order_Processing_Fee                     DECIMAL(10, 2)                  COMMENT '订单处理费',
        Commission_Fee                                  DECIMAL(10, 2)                  COMMENT '佣金',
        Transaction_Fee                                 DECIMAL(10, 2)                  COMMENT '交易费',
        Service_Fee                                     DECIMAL(10, 2)                  COMMENT '服务费', 
        Vat                                             DECIMAL(10, 2)                  COMMENT '增值税#映射到税费',
        Delivery_Seller_Protection_Fee_Premium_Amount   DECIMAL(10, 2)                  COMMENT '配送卖家保护费高级金额',
        Seller_Coin_Cash_Back                           DECIMAL(10, 2)                  COMMENT '卖家币返还',
        Return_Quantity                                 INT                             COMMENT '退货数量',
        Return_Price                                    DECIMAL(10, 2)                  COMMENT '退款金额',
        Return_Seller_Voucher                           DECIMAL(10, 2)                  COMMENT '退还卖家优惠券金额',                                              
        Return_Shipping_Cost                            DECIMAL(10, 2)                  COMMENT '退货运费',                      
        PRIMARY KEY (Order_ID, Product_ID)
) COMMENT='shopee原始数据';

create table if not exists lazada_orders(
Purchase_Datetime                                   DATETIME NOT NULL       COMMENT '订购时间'                ,                   -- (订购时间)
Order_ID                                            VARCHAR(50) NOT NULL    COMMENT '系统单号'                          ,                   -- (系统单号)
Region                                              VARCHAR(50)             COMMENT '大区'                        ,         -- 推荐：对应“大区”，如果需要单独存储区域信息    
Ship_Country_Name                                   VARCHAR(20)             COMMENT '国家中文名'                ,                   -- (国家/地区)
Ship_Country                                        CHAR(2)                 COMMENT '国家'                ,                   -- (国家/地区)
Currency                                            CHAR(3)                 COMMENT '订单币种'                ,                   -- (订单币种)
Exchange_Rate                                       DECIMAL(10, 6)          COMMENT '汇率', 
Fulfillment_Channel                                 VARCHAR(20)             COMMENT '平台 履行渠道'                     ,                   -- (平台)履行渠道
Store                                               VARCHAR(255) NOT NULL   COMMENT '店铺'                              ,                   -- (店铺)表格没有 手动添加
Tags                                                VARCHAR(255)            COMMENT '父ASIN/spuSPU标签 Parent SKU Reference No'         ,                   -- (标签)Parent SKU Reference No
Product_ID                                          VARCHAR(255) NOT NULL   COMMENT '子ASIN/sku商品Id'                  ,                   -- (商品Id)
MSKU                                                VARCHAR(255)            COMMENT 'MSKU'                    ,                   -- (MSKU)
Product_Chinese_Name                                VARCHAR(255)            COMMENT '中文品名',
Product_Name                                        VARCHAR(255)            COMMENT '品名',           
Variation_Name                                      VARCHAR(255)            COMMENT '规格',          
Quantity                                            INT                     COMMENT '销量',    
Item_Price                                          DECIMAL(10, 2)          COMMENT '商品金额', 
Seller_Voucher                                      DECIMAL(10, 2)          COMMENT '卖家优惠券金额', 
Platform_Voucher                                    DECIMAL(10, 2)          COMMENT '平台优惠券', 
Buyer_Paid_Shipping_Fee                             DECIMAL(10, 2)          COMMENT '买家支付运费', 
Actual_Shipping_Fee                                 DECIMAL(10, 2)          COMMENT '实际物流运费', 
Shipping_Subsidy                                    DECIMAL(10, 2)          COMMENT '运费返还', 
Affiliate_Marketing                                 DECIMAL(10, 2)          COMMENT '联盟营销方案佣金', 
Commission_Fee                                      DECIMAL(10, 2)          COMMENT '佣金'                      ,                   -- 佣金
Transaction_Fee                                     DECIMAL(10, 2)          COMMENT '商品交易费'               ,                   -- (商品交易费)
Return_Quantity                                     INT                     COMMENT '退货数量',
Return_Price                                        DECIMAL(10, 2)          COMMENT '退款金额',
Return_Credit                                       DECIMAL(10, 2)          COMMENT '退款入账',
Return_Debit                                        DECIMAL(10, 2)          COMMENT '退款支出',
Product_Cost                                        DECIMAL(10, 2)          COMMENT '产品成本'                    ,         --     产品成本: "" # 无直接对应
First_Leg_Shipping_Cost                             DECIMAL(10, 2)          COMMENT '头程',
Shipping_Cost                                       DECIMAL(10, 2)          COMMENT '运费非平台'                ,                   -- (物流运费)
Advertising_Spend                                   DECIMAL(10, 2)          COMMENT '广告花费'                    ,         
Lost_Claim                                          DECIMAL(10, 2)          COMMENT '丢件索赔',
Other_Debit                                         DECIMAL(10, 2)          COMMENT '其它支出',

        PRIMARY KEY (Order_ID, Product_ID)               
        )COMMENT='lazada总表';


create table if not exists exchange_rate(
        Currency                                        CHAR(3) NOT NULL        COMMENT '币种代码，如 USD', 
        Bank_Buy_rate                                   DECIMAL(10, 6) NOT NULL COMMENT '现汇买入价',
        Rate_Datetime                                   DATETIME NOT NULL       COMMENT '汇率日期时间',
        PRIMARY KEY (Currency, Rate_Datetime)

) COMMENT='外汇 数据来源于中国银行https://www.boc.cn/sourcedb/whpj/index.html';

