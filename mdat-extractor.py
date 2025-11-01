import os
import glob
import pandas as pd
import zipfile  # 用于读取 .mdat (zip) 文件
import io       # 用于在内存中处理文件
import re

# --- 1. 配置区：请修改这里的路径 ---

# 你的【原始 .mdat】文件存放路径 (请使用 / )
INPUT_FOLDER = './Input_Data_MDAT' 

# 你希望保存结果的路径 (如果不存在，脚本会自动创建)
OUTPUT_FOLDER = './Output_Data'

# 我们要提取和重命名的列 (基于 MultiStat 文档)
# 格式: {'原始列名': '重命名后的列名'}
TARGET_COLUMNS_MAP = {
    'Freq(Hz)': 'frequency (Hz)',
    "Z'(a)": 'Z_real (Ohm)',
    "Z''(b)": 'Z_imag (Ohm)' # (v6 修复: 现在将保留原始正负号)
}

# --- 2. 核心解析函数 ---

def parse_ascii_content(file_content, output_base_name, output_dir):
    """
    解析解压后的 ASCII 文本内容 (来自 .z 或 .Z 文件)。
    """
    try:
        # 1. 将内存中的内容分割成行
        lines = file_content.splitlines(True) # True 保留换行符
        
        # 2. 查找 'End Header:'
        end_header_index = -1
        for i, line in enumerate(lines):
            if 'End Header:' in line:
                end_header_index = i
                break
        
        if end_header_index == -1:
            print(f"    [!] 跳过子文件: 未找到 'End Header:'。")
            return

        # 3. 找到表头行 (End Header 往上数第3行, 索引-2)
        header_line_index = end_header_index - 2 
        if header_line_index < 0:
            print(f"    [!] 跳过子文件: 文件头信息过短。")
            return
            
        header_line = lines[header_line_index].strip()
        column_names = re.split(r'\s+', header_line)

        # 4. 找到数据起始行
        data_start_line = end_header_index + 1
        if data_start_line >= len(lines):
            print(f"    [!] 跳过子文件: 找到 'End Header:' 但之后没有数据。")
            return

        # 5. 使用 Pandas 解析数据
        data_io = io.StringIO("".join(lines[data_start_line:]))
        
        df = pd.read_csv(
            data_io,
            sep=r'\s+',
            names=column_names,
            header=None,
            engine='python',
            on_bad_lines='skip'
        )
        
        # 6. 提取并重命名目标列
        extracted_df = pd.DataFrame()
        missing_cols = []
        
        for original_name, new_name in TARGET_COLUMNS_MAP.items():
            if original_name in df.columns:
                data_series = pd.to_numeric(df[original_name], errors='coerce')
                
                # --- (!!!) 修复 (v6) (!!!) ---
                #
                # 移除了 .abs() 函数，以保留 Z''(b) 的原始正负号。
                extracted_df[new_name] = data_series
                #
                # --- 修复结束 ---
                
            else:
                missing_cols.append(original_name)

        if missing_cols:
            print(f"    [!] 警告: 未在子文件中找到列: {', '.join(missing_cols)}")

        # 7. 清理并保存
        extracted_df = extracted_df.dropna()
        if extracted_df.empty:
            print(f"    [!] 跳过子文件: 提取了列，但没有有效的数值数据。")
            return

        # 8. 生成输出文件名并保存
        output_csv_path = os.path.join(output_dir, f"{output_base_name}_extracted.csv")
        output_txt_path = os.path.join(output_dir, f"{output_base_name}_extracted.txt")
        
        extracted_df.to_csv(output_csv_path, index=False, encoding='utf-8')
        extracted_df.to_csv(output_txt_path, index=False, sep='\t', encoding='utf-8')
        
        print(f"    [✓] 成功: 提取 {len(extracted_df)} 行数据 -> {os.path.basename(output_csv_path)}")

    except PermissionError: 
        print(f"    [X] 失败: 权限被拒绝 (Permission Denied)。")
        print(f"        -> 请检查文件是否已被 Excel 或其他程序打开: {os.path.basename(output_csv_path)}")
    except pd.errors.EmptyDataError:
        print(f"    [!] 跳过子文件: 文件中没有可解析的数据。")
    except Exception as e:
        print(f"    [X] 失败: 解析 ASCII 内容时出错: {e}")

# --- 3. 批量处理 .mdat (ZIP) 文件 ---

def process_mdat_file(file_path, output_dir):
    """
    打开 .mdat (ZIP) 文件并处理其中的 AC 数据子文件。
    """
    base_name = os.path.basename(file_path)
    print(f"\n--- 正在处理: {base_name} ---")
    
    # 获取 .mdat 文件的基础名 (例如: "eis-0.5H-1.0air-0atm-12mV-wet75-2")
    mdat_base = os.path.splitext(base_name)[0]
    
    try:
        if not zipfile.is_zipfile(file_path):
            print(f"[!] 跳过: {base_name} 不是一个有效的 ZIP (.mdat) 文件。")
            return

        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            # 查找所有 AC 数据文件 (以 .z 或 .Z 结尾)
            ac_files_in_zip = [f for f in zip_ref.namelist() if f.lower().endswith('.z')]
            
            if not ac_files_in_zip:
                print(f"[!] 信息: 在 {base_name} 中未找到 AC 数据文件 (.z 或 .Z)。")
                return

            print(f"    在 .mdat 中找到 {len(ac_files_in_zip)} 个 AC 数据子文件。")
            
            for sub_file_name in ac_files_in_zip:
                print(f"    -> 正在解析子文件: {sub_file_name}")
                
                # 1. 从 ZIP 中读取子文件到内存
                file_data = zip_ref.read(sub_file_name)
                
                # 2. 解码 (仪器文件常用 latin-1)
                try:
                    file_content = file_data.decode('latin-1')
                except UnicodeDecodeError:
                    try:
                        file_content = file_data.decode('utf-8')
                    except Exception as e:
                        print(f"    [X] 失败: 解码 {sub_file_name} 出错: {e}")
                        continue
                
                # 3. 采用 v5 的命名逻辑
                # 从子文件名 (如 "Run01/...") 中提取 "RunXX"
                run_match = re.search(r'Run(\d+)', sub_file_name, re.IGNORECASE)
                
                if run_match:
                    run_str = run_match.group(0) # 结果为 'Run01' 或 'Run02'
                    # 新逻辑: [MDAT_BaseName]-[RunXX]
                    output_base_name = f"{mdat_base}-{run_str}"
                
                else:
                    # (备用逻辑)
                    sub_file_base = sub_file_name.replace('/', '_')
                    output_base_name = os.path.splitext(sub_file_base)[0]

                # 4. 调用核心解析函数
                parse_ascii_content(file_content, output_base_name, output_dir)

    except zipfile.BadZipFile:
        print(f"[X] 失败: {base_name} 是一个损坏的 ZIP (.mdat) 文件。")
    except Exception as e:
        print(f"[X] 失败: 处理 {base_name} 时发生未知错误: {e}")

# --- 4. 主函数 ---

def main():
    """
    主函数，用于扫描和批量处理。
    """
    print(f"--- 自动化 EIS 批量处理 (v6 - 修复 Z'' 正负号) ---")
    
    if not os.path.exists(INPUT_FOLDER):
        print(f"输入目录 {INPUT_FOLDER} 不存在，已自动创建。")
        print("请将你的【.mdat】文件放入该目录后重新运行脚本。")
        os.makedirs(INPUT_FOLDER)
        return
        
    if not os.path.exists(OUTPUT_FOLDER):
        print(f"创建输出目录: {OUTPUT_FOLDER}")
        os.makedirs(OUTPUT_FOLDER)
    
    # 查找所有 .mdat 文件 (不区分大小写)
    search_path_lower = os.path.join(INPUT_FOLDER, "*.mdat")
    search_path_upper = os.path.join(INPUT_FOLDER, "*.MDAT")
    files_to_process = glob.glob(search_path_lower) + glob.glob(search_path_upper)
    
    files_to_process = list(set(files_to_process)) # 去重

    if not files_to_process:
        print(f"\n[!] 错误: 在 {INPUT_FOLDER} 中未找到任何 .mdat 文件。")
        print(f"    请确保你的【.mdat】文件已放入该目录。")
        return

    print(f"\n在 {INPUT_FOLDER} 中找到 {len(files_to_process)} 个 .mdat 文件。开始处理...")
    
    for file_path in files_to_process:
        process_mdat_file(file_path, OUTPUT_FOLDER)
        
    print("\n--- 批量处理完成 ---")

if __name__ == "__main__":
    main()