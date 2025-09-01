import os
import sqlite3
import ftplib
import py7zr
import pandas as pd
import re
import time
import socket
import multiprocessing
from queue import Empty

# As funções worker são executadas em processos separados

def worker_download(ftp_host, ftp_path, dest, year, queue, result_queue):
    """Baixa um único arquivo. Executado em um processo separado."""
    ftp = None
    try:
        ftp = ftplib.FTP(ftp_host, timeout=10)
        ftp.login()
        if ftp.sock:
            ftp.sock.settimeout(15.0)
        ftp.set_pasv(True)
        ftp.cwd(os.path.dirname(ftp_path))
        file_name = os.path.basename(ftp_path)
        total_size = ftp.size(file_name)
        queue.put(("FILE_PROGRESS_START", {"file": file_name, "total_size": total_size}))
        
        class ProgressTracker:
            def __init__(self, q):
                self.q = q
                self.bytes_so_far = 0
            def __call__(self, chunk):
                f.write(chunk)
                self.bytes_so_far += len(chunk)
                self.q.put(("FILE_PROGRESS_UPDATE", {"file": file_name, "bytes_downloaded": self.bytes_so_far}))

        with open(dest, 'wb') as f:
            tracker = ProgressTracker(queue)
            ftp.retrbinary(f'RETR {file_name}', tracker, blocksize=1048576)
        ftp.quit()

        if os.path.getsize(dest) != total_size:
             raise Exception("Tamanho do arquivo final não confere com o original.")

        result_queue.put((True, dest, year))
    except Exception as e:
        if ftp:
            try: ftp.close()
            except: pass
        if os.path.exists(dest):
            os.remove(dest)
        result_queue.put((False, dest, str(e)))

def worker_decompress(path, out_dir, queue):
    """Descomprime um arquivo .7z."""
    try:
        with py7zr.SevenZipFile(path, mode='r') as z:
            z.extractall(path=out_dir)
            queue.put(("LOG", f"OK: {os.path.basename(path)} descomprimido."))
    except Exception as e:
        queue.put(("LOG", f"Erro ao descomprimir {path}: {e}"))

def worker_process_db(txt_path, year, conn_str, is_first, queue, selected_columns=None):
    """Processa um arquivo de texto e o insere no banco de dados."""
    conn = sqlite3.connect(conn_str)
    try:
        chunk_size = 500000
        year_str = str(year)
        dtype_map = {'10': str, '11': str}
        
        read_csv_args = {
            'sep': ';', 'encoding': 'latin-1', 'low_memory': False,
            'chunksize': chunk_size, 'dtype': dtype_map, 'on_bad_lines': 'warn'
        }
        if selected_columns:
            read_csv_args['usecols'] = selected_columns

        for i, chunk in enumerate(pd.read_csv(txt_path, **read_csv_args)):
            queue.put(("LOG", f"  - Processando chunk {i+1} de {os.path.basename(txt_path)}..."))
            chunk['ano'] = year_str
            new_cols = {col: re.sub(r'[^\w]', '', col.strip().replace(' ', '_')) for col in chunk.columns}
            chunk.rename(columns=new_cols, inplace=True)
            if_exists = 'replace' if is_first and i == 0 else 'append'
            chunk.to_sql(DownloadManager.NOME_TABELA_FINAL, conn, if_exists=if_exists, index=False)
    except Exception as e:
        queue.put(("LOG", f"  - Erro ao processar {os.path.basename(txt_path)}: {e}"))
    finally:
        conn.close()

class DownloadManager:
    DATA_DIR = "data"
    DB_PATH = os.path.join(DATA_DIR, "rais.db")
    NOME_TABELA_FINAL = "vinculos"

    def __init__(self, queue):
        self.queue = queue
        self.active_processes = []
        self._cancel_requested = multiprocessing.Event()

    def cancel_active_downloads(self):
        self.queue.put(("LOG", "Cancelamento solicitado..."))
        self._cancel_requested.set()
        for p in self.active_processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=1)
        self.active_processes = []
        self.queue.put(("LOG", "Processos de download terminados."))

    def start_processing(self, years, files, available_data):
        self._cancel_requested.clear()
        try:
            if not os.path.exists(self.DATA_DIR): os.makedirs(self.DATA_DIR)
            
            tasks = self._prepare_tasks(years, files, available_data)
            if not tasks:
                self.queue.put(("LOG", "Nenhum arquivo válido encontrado."))
                return

            downloaded_files = self._execute_downloads(tasks)
            
            if self._cancel_requested.is_set():
                self.queue.put(("LOG", "Processo interrompido."))
                return

            if downloaded_files:
                self._process_files_to_db(downloaded_files)

        except Exception as e:
            self.queue.put(("LOG", f"[ERRO GERAL] {e}"))
        finally:
            if not self._cancel_requested.is_set():
                 self.queue.put(("DONE", None))

    def _execute_downloads(self, tasks):
        self.queue.put(("TOTAL_PROGRESS_MAX", len(tasks)))
        self.queue.put(("LOG", f"Iniciando download sequencial de {len(tasks)} arquivos..."))
        
        downloaded_files = []
        for i, task in enumerate(tasks):
            if self._cancel_requested.is_set():
                break

            self.queue.put(("LOG", f"Iniciando arquivo {i+1}/{len(tasks)}: {os.path.basename(task[1])}"))
            
            result_queue = multiprocessing.Queue(maxsize=1)
            ftp_host, ftp_path, dest, year, queue = task
            process_args = (ftp_host, ftp_path, dest, year, queue, result_queue)

            process = multiprocessing.Process(target=worker_download, args=process_args)
            self.active_processes = [process]
            process.start()

            # Block until result is available or cancel is requested
            while result_queue.empty():
                if self._cancel_requested.is_set():
                    process.terminate()
                    process.join(timeout=1)
                    break
                time.sleep(0.1)

            if not self._cancel_requested.is_set():
                if not result_queue.empty():
                    success, path, result = result_queue.get()
                    if success:
                        self.queue.put(("LOG", f"OK: {os.path.basename(path)}"))
                        downloaded_files.append((path, result))
                    else:
                        self.queue.put(("LOG", f"FALHA: {os.path.basename(path)} - {result}"))

            process.join()
            self.active_processes = []

            if self._cancel_requested.is_set():
                break

            self.queue.put(("TOTAL_PROGRESS_UPDATE", i + 1))

        return downloaded_files

    def _process_files_to_db(self, downloaded_files):
        self.queue.put(("LOG", "\nIniciando descompactação..."))
        for path_7z, year in sorted(downloaded_files, key=lambda x: (x[1], x[0])):
            if self._cancel_requested.is_set(): break
            
            self.queue.put(("LOG", f"Descompactando: {os.path.basename(path_7z)}"))
            # A descompressão é rápida, pode ser feita no thread principal
            worker_decompress(path_7z, self.DATA_DIR, self.queue)
            try:
                os.remove(path_7z)
            except OSError as e:
                self.queue.put(("LOG", f"Aviso: Não foi possível remover o arquivo baixado {path_7z}: {e}"))
        self.queue.put(("LOG", "Descompactação concluída."))

    def process_single_file_to_db(self, txt_path, year, selected_columns):
        self.queue.put(("LOG", f"Iniciando importação de {os.path.basename(txt_path)} em processo separado..."))
        task_args = (txt_path, year, self.DB_PATH, True, self.queue, selected_columns)
        proc = multiprocessing.Process(target=worker_process_db, args=task_args)
        self.active_processes = [proc]
        proc.start()
        # Não bloqueia, a UI vai receber o DONE da fila

    def _prepare_tasks(self, years, files, available_data):
        self.queue.put(("LOG", f"[Manager] Preparando tarefas para Anos: {years} e {len(files)} arquivos selecionados."))
        tasks = []
        for year in years:
            year_data = available_data.get(year)
            if not year_data: continue
            
            dir_name = year_data['dir']
            for file_name in files:
                if file_name in year_data['files']:
                    ftp_path = f"/pdet/microdados/RAIS/{dir_name}/{file_name}"
                    local_path = os.path.join(self.DATA_DIR, f"{year}_{os.path.basename(file_name)}")
                    task_tuple = (FTPService.FTP_HOST, ftp_path, local_path, year, self.queue)
                    if task_tuple not in tasks:
                        tasks.append(task_tuple)
        self.queue.put(("LOG", f"[Manager] {len(tasks)} tarefas criadas."))
        return tasks

# FTPService pode ser uma classe simples ou apenas constantes
class FTPService:
    FTP_HOST = "ftp.mtps.gov.br"