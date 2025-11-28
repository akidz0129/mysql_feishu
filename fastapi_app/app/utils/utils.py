import csv
import io
def generate_csv_content(data, fields):
    """
    生成CSV内容的通用函数
    :param data: 包含数据的列表，每个元素可以是字典或元组/列表
    :param fields: 字段名列表，用于CSV的表头和确定数据顺序
    """
    output = io.StringIO()
    writer = csv.writer(output)
  # *** 关键：在文件开头写入 UTF-8 BOM ***
    # '\ufeff' 是 UTF-8 BOM 的 Unicode 字符表示
    output.write('\ufeff') 

    # 写入表头
    writer.writerow(fields)

    # 写入数据行
    for row_data in data:
        if isinstance(row_data, dict):
            # 如果数据是字典，按fields顺序提取值
            row = [row_data.get(field, '') for field in fields]
        else:
            # 如果数据是元组/列表，假设顺序与fields一致
            row = row_data
        writer.writerow(row)
    
    return output.getvalue()
