import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
import multiprocessing
from queue import Empty
import threading
import time
import os
import pandas as pd
import re
from PIL import Image, ImageTk

from src.controllers.ftp_service import FTPService
from src.controllers.download_manager import DownloadManager

class ScrollableFrame(ttk.Frame):
    """Um frame com uma barra de rolagem vertical."""
    def __init__(self, container, *args, **kwargs):
        super().__init__(container, *args, **kwargs)
        canvas = tk.Canvas(self, borderwidth=0, background="#ffffff")
        self.scrollable_frame = ttk.Frame(canvas)
        scrollbar = ttk.Scrollbar(self, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set)

        scrollbar.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)
        canvas_window = canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")

        def on_frame_configure(event):
            canvas.configure(scrollregion=canvas.bbox("all"))

        def on_canvas_configure(event):
            canvas.itemconfig(canvas_window, width=event.width)

        self.scrollable_frame.bind("<Configure>", on_frame_configure)
        canvas.bind("<Configure>", on_canvas_configure)

class MainApplicationWindow:
    """A classe principal da UI, focada em widgets e eventos."""
    def __init__(self, root, queue, fetched_data=None):
        self.root = root
        self.queue = queue
        self.root.title("RAIS Microdata-Tool")
        self.root.state('zoomed')

        self.available_data = {}
        self.file_vars = {}
        self._years_last_width = 0

        self.current_file_total_size = 0
        self.current_file_bytes_downloaded = 0
        self.current_file_start_time = 0
        self.last_update_time = 0
        self.last_update_bytes = 0

        self.selected_processing_file = None
        self.column_vars = {}

        self.ftp_service = FTPService(self.queue)
        self.download_manager = DownloadManager(self.queue)

        self._create_main_widgets()
        
        if fetched_data:
            self.populate_initial_data(fetched_data)
        
        self.process_queue()

    def populate_initial_data(self, data):
        # self.loading_label.pack_forget() # This label is not used anymore
        self.available_data = data
        self.year_vars = {year: tk.BooleanVar(value=False) for year in sorted(self.available_data.keys(), reverse=True)}
        self._update_file_list()
        self.years_items_frame.update_idletasks()
        self._redraw_year_checkboxes()
        self.log("Dados do servidor carregados. Por favor, faça suas seleções.")

    def _center_window(self, width, height):
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        center_x = int(screen_width/2 - width / 2)
        center_y = int(screen_height/2 - height / 2)
        self.root.geometry(f'{width}x{height}+{center_x}+{center_y}')

    def _create_main_widgets(self):
        self.main_frame = ttk.Frame(self.root, padding="10")
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        
        self.notebook = ttk.Notebook(self.main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        self.download_tab = ttk.Frame(self.notebook)
        self.processing_tab = ttk.Frame(self.notebook)
        self.support_tab = ttk.Frame(self.notebook)

        self.download_tab.pack(fill=tk.BOTH, expand=True)
        self.processing_tab.pack(fill=tk.BOTH, expand=True)
        self.support_tab.pack(fill=tk.BOTH, expand=True)

        self.notebook.add(self.download_tab, text="Download")
        self.notebook.add(self.processing_tab, text="Exportação de Dados")
        self.notebook.add(self.support_tab, text="Suporte")

        self._create_download_tab_widgets(self.download_tab)
        self._create_processing_tab_widgets(self.processing_tab)
        self._create_support_tab_widgets(self.support_tab)

        # self.loading_label = ttk.Label(self.main_frame, text="Buscando dados no servidor FTP, por favor aguarde...")
        # self.loading_label.pack(pady=20)

    def _refresh_extracted_files_list(self):
        self.extracted_files_listbox.delete(0, tk.END)
        data_dir = DownloadManager.DATA_DIR
        if not os.path.exists(data_dir): os.makedirs(data_dir)

        txt_files = [f for f in os.listdir(data_dir) if f.endswith(".txt")]
        for f in sorted(txt_files):
            self.extracted_files_listbox.insert(tk.END, f)

    def _on_file_selected(self, event):
        for widget in self.columns_checkbox_frame.scrollable_frame.winfo_children():
            widget.destroy()
        self.column_vars.clear()

        selected_indices = self.extracted_files_listbox.curselection()
        if not selected_indices: return

        selected_filename = self.extracted_files_listbox.get(selected_indices[0])
        self.selected_processing_file = os.path.join(DownloadManager.DATA_DIR, selected_filename)

        try:
            df = pd.read_csv(self.selected_processing_file, sep=';', encoding='latin-1', nrows=0)
            columns = df.columns.tolist()

            for col in columns:
                var = tk.BooleanVar(value=True)
                self.column_vars[col] = var
                cb = ttk.Checkbutton(self.columns_checkbox_frame.scrollable_frame, text=col, variable=var)
                cb.pack(anchor="w", padx=5)
        except Exception as e:
            self.log(f"[ERROR] Erro ao ler colunas do arquivo {selected_filename}: {e}")
            messagebox.showerror("Erro", f"Não foi possível ler as colunas do arquivo {selected_filename}. Erro: {e}")

    def _export_to_sqlite(self, selected_columns):
        if not self.selected_processing_file:
            messagebox.showwarning("Aviso", "Selecione um arquivo para exportar.")
            return

        if not selected_columns:
            messagebox.showwarning("Aviso", "Selecione pelo menos uma coluna para exportar.")
            return

        try:
            # Read the selected file into a pandas DataFrame, selecting only the chosen columns
            df = pd.read_csv(self.selected_processing_file, sep=';', encoding='latin-1', usecols=selected_columns)

            file_basename = os.path.basename(self.selected_processing_file)
            default_filename = os.path.splitext(file_basename)[0] + ".db"

            filepath = filedialog.asksaveasfilename(
                defaultextension=".db",
                filetypes=[("SQLite database files", "*.db"), ("All files", "*.* ")],
                initialfile=default_filename
            )

            if not filepath: # User cancelled the dialog
                self.log("Exportação para SQLite cancelada pelo usuário.")
                return

            # Save DataFrame to SQLite
            # You might need to install sqlalchemy and sqlite3 if not already installed
            # pip install sqlalchemy
            # import sqlite3 # sqlite3 is usually built-in with Python
            from sqlalchemy import create_engine
            engine = create_engine(f'sqlite:///{filepath}')
            table_name = os.path.splitext(file_basename)[0] # Use filename as table name
            df.to_sql(table_name, engine, if_exists='replace', index=False)

            self.log(f"Dados exportados com sucesso para {filepath} na tabela {table_name}.")
            messagebox.showinfo("Sucesso", f"Dados exportados com sucesso para SQLite:\n{filepath}\nTabela: {table_name}")

        except Exception as e:
            self.log(f"[ERROR] Erro ao exportar dados para SQLite: {e}")
            messagebox.showerror("Erro de Exportação", f"Ocorreu um erro ao exportar os dados para SQLite: {e}")

    def _show_export_options_dialog(self):
        if not self.selected_processing_file:
            messagebox.showwarning("Aviso", "Selecione um arquivo para exportar.")
            return

        selected_columns = [col for col, var in self.column_vars.items() if var.get()]
        if not selected_columns:
            messagebox.showwarning("Aviso", "Selecione pelo menos uma coluna para exportar.")
            return

        dialog = tk.Toplevel(self.root)
        dialog.title("Opções de Exportação")
        dialog.transient(self.root)
        dialog.grab_set()
        dialog.focus_set()

        # Center the dialog
        dialog_width = 300
        dialog_height = 200
        self.root.update_idletasks()
        x = self.root.winfo_x() + (self.root.winfo_width() // 2) - (dialog_width // 2)
        y = self.root.winfo_y() + (self.root.winfo_height() // 2) - (dialog_height // 2)
        dialog.geometry(f'{dialog_width}x{dialog_height}+{x}+{y}')

        format_var = tk.StringVar(value="SQLite") # Default selection

        ttk.Label(dialog, text="Selecione o formato de exportação:").pack(pady=10)

        formats = ["SQLite", "TXT", "CSV", "EXCEL"]
        for fmt in formats:
            ttk.Radiobutton(dialog, text=fmt, variable=format_var, value=fmt).pack(anchor="w", padx=20)

        def on_export():
            selected_format = format_var.get()
            dialog.destroy()
            self._perform_export(selected_format, selected_columns)

        ttk.Button(dialog, text="Exportar", command=on_export).pack(pady=10)

    def _perform_export(self, export_format, selected_columns):
        if not self.selected_processing_file:
            messagebox.showwarning("Aviso", "Nenhum arquivo selecionado para exportar.")
            return

        try:
            # Read the selected file into a pandas DataFrame, selecting only the chosen columns
            df = pd.read_csv(self.selected_processing_file, sep=';', encoding='latin-1', usecols=selected_columns)

            file_basename = os.path.basename(self.selected_processing_file)
            default_filename = os.path.splitext(file_basename)[0] + "_exported"

            filetypes = []
            if export_format == "SQLite":
                self._export_to_sqlite(selected_columns) # Pass selected_columns
                return
            elif export_format == "TXT":
                filetypes = [("Text files", "*.txt"), ("All files", "*.* ")]
                default_extension = ".txt"
            elif export_format == "CSV":
                filetypes = [("CSV files", "*.csv"), ("All files", "*.* ")]
                default_extension = ".csv"
            elif export_format == "EXCEL":
                filetypes = [("Excel files", "*.xlsx"), ("All files", "*.* ")]
                default_extension = ".xlsx"
            else:
                messagebox.showerror("Erro", "Formato de exportação inválido.")
                return

            # Open save file dialog
            filepath = filedialog.asksaveasfilename(
                defaultextension=default_extension,
                filetypes=filetypes,
                initialfile=default_filename
            )

            if not filepath: # User cancelled the dialog
                self.log("Exportação cancelada pelo usuário.")
                return

            # Perform the export
            if export_format == "TXT":
                df.to_csv(filepath, sep='\t', index=False)
            elif export_format == "CSV":
                df.to_csv(filepath, index=False)
            elif export_format == "EXCEL":
                df.to_excel(filepath, index=False)

            self.log(f"Dados exportados com sucesso para {filepath} em formato {export_format}.")
            messagebox.showinfo("Sucesso", f"Dados exportados com sucesso para:\n{filepath}")

        except Exception as e:
            self.log(f"[ERROR] Erro ao exportar dados para {export_format}: {e}")
            messagebox.showerror("Erro de Exportação", f"Ocorreu um erro ao exportar os dados: {e}")

    def _format_bytes(self, bytes_val):
        if bytes_val < 1024: return f"{bytes_val:.0f} B"
        elif bytes_val < 1024**2: return f"{bytes_val/1024:.1f} KB"
        elif bytes_val < 1024**3: return f"{bytes_val/1024**2:.1f} MB"
        else: return f"{bytes_val/1024**3:.1f} GB"

    def _create_download_tab_widgets(self, parent_frame):
        parent_frame.columnconfigure(0, weight=1)
        parent_frame.rowconfigure(1, weight=1)
        parent_frame.rowconfigure(4, weight=1)

        options_frame = ttk.LabelFrame(parent_frame, text="Opções de Download")
        options_frame.grid(row=0, column=0, sticky="ew", pady=5)
        options_frame.columnconfigure(1, weight=1)

        years_label = ttk.Label(options_frame, text="Anos:", width=10)
        years_label.grid(row=0, column=0, sticky="n", padx=5, pady=5)
        
        self.year_vars = {year: tk.BooleanVar(value=False) for year in self.available_data.keys()}
        
        self.years_items_frame = ttk.Frame(options_frame)
        self.years_items_frame.grid(row=0, column=1, sticky="ew")
        self.years_items_frame.bind("<Configure>", self._redraw_year_checkboxes)

        files_frame = ttk.LabelFrame(parent_frame, text="Arquivos Disponíveis")
        files_frame.grid(row=1, column=0, sticky="nsew", pady=5)
        files_frame.columnconfigure(0, weight=1)
        files_frame.rowconfigure(0, weight=1)
        
        self.scrollable_files_frame = ScrollableFrame(files_frame)
        self.scrollable_files_frame.grid(row=0, column=0, sticky="nsew")

        controls_frame = ttk.Frame(parent_frame)
        controls_frame.grid(row=2, column=0, sticky="ew")
        controls_frame.columnconfigure(0, weight=1)

        button_container = ttk.Frame(controls_frame)
        button_container.pack(side=tk.RIGHT, padx=5, pady=10)

        self.cancel_button = ttk.Button(button_container, text="Cancelar", command=self._request_cancel, state="disabled")
        self.cancel_button.pack(side=tk.RIGHT, padx=(5,0))

        self.start_button = ttk.Button(button_container, text="Iniciar Download", command=self._start_processing_thread)
        self.start_button.pack(side=tk.RIGHT)

        progress_details_frame = ttk.LabelFrame(parent_frame, text="Progresso do Download")
        progress_details_frame.grid(row=3, column=0, sticky="ew", pady=5)
        progress_details_frame.columnconfigure(0, weight=1)

        self.current_file_label = ttk.Label(progress_details_frame, text="Arquivo: N/A")
        self.current_file_label.grid(row=0, column=0, sticky="w", padx=5, pady=2)

        self.file_progress = ttk.Progressbar(progress_details_frame, orient="horizontal", mode="determinate")
        self.file_progress.grid(row=1, column=0, sticky="ew", padx=5, pady=2)

        self.size_label = ttk.Label(progress_details_frame, text="Tamanho: N/A")
        self.size_label.grid(row=2, column=0, sticky="w", padx=5, pady=2)

        self.speed_label = ttk.Label(progress_details_frame, text="Velocidade: N/A")
        self.speed_label.grid(row=3, column=0, sticky="w", padx=5, pady=2)

        self.overall_progress_label = ttk.Label(progress_details_frame, text="Progresso Total: N/A")
        self.overall_progress_label.grid(row=4, column=0, sticky="w", padx=5, pady=2)
        self.overall_progress = ttk.Progressbar(progress_details_frame, orient="horizontal", mode="determinate")
        self.overall_progress.grid(row=5, column=0, sticky="ew", padx=5, pady=2)

        self.status_area = scrolledtext.ScrolledText(parent_frame, height=8, state="disabled")
        self.status_area.grid(row=4, column=0, sticky="nsew")

    def _create_processing_tab_widgets(self, parent_frame):
        parent_frame.columnconfigure(0, weight=1)
        parent_frame.rowconfigure(1, weight=1) # Manter weight=1 para a lista de arquivos
        parent_frame.rowconfigure(2, weight=1) # Manter weight=1 para as colunas
        parent_frame.rowconfigure(3, weight=0) # Nova row para o botão Exportar Dados

        # Frame para o botão de atualização (agora na row 0)
        refresh_button_frame = ttk.Frame(parent_frame)
        refresh_button_frame.grid(row=0, column=0, pady=10, sticky="ew")
        refresh_button_frame.columnconfigure(0, weight=1) # Para centralizar o botão

        refresh_button = ttk.Button(refresh_button_frame, text="Atualizar Arquivos", command=self._refresh_extracted_files_list)
        refresh_button.grid(row=0, column=0, padx=5, pady=0) # Usar grid para centralizar no frame

        extracted_files_frame = ttk.LabelFrame(parent_frame, text="Arquivos .txt Extraídos")
        extracted_files_frame.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)
        extracted_files_frame.columnconfigure(0, weight=1)
        extracted_files_frame.rowconfigure(0, weight=1)

        self.extracted_files_listbox = tk.Listbox(extracted_files_frame, selectmode=tk.SINGLE)
        self.extracted_files_listbox.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
        self.extracted_files_listbox.bind("<<ListboxSelect>>", self._on_file_selected)

        files_scrollbar = ttk.Scrollbar(extracted_files_frame, orient="vertical", command=self.extracted_files_listbox.yview)
        files_scrollbar.grid(row=0, column=1, sticky="ns")
        self.extracted_files_listbox.config(yscrollcommand=files_scrollbar.set)

        columns_frame = ttk.LabelFrame(parent_frame, text="Colunas Disponíveis")
        columns_frame.grid(row=2, column=0, sticky="nsew", padx=5, pady=5)
        columns_frame.columnconfigure(0, weight=1)
        columns_frame.rowconfigure(0, weight=1)

        self.columns_checkbox_frame = ScrollableFrame(columns_frame)
        self.columns_checkbox_frame.grid(row=0, column=0, sticky="nsew")

        # Botão Exportar Dados (agora na row 3)
        import_button = ttk.Button(parent_frame, text="Exportar Dados", command=self._show_export_options_dialog)
        import_button.grid(row=3, column=0, pady=10) # Alterado para row=3

        self._refresh_extracted_files_list()

    def _create_support_tab_widgets(self, parent_frame):
        parent_frame.columnconfigure(0, weight=1)
        parent_frame.rowconfigure(0, weight=1)

        content_frame = ttk.Frame(parent_frame)
        content_frame.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        content_frame.columnconfigure(0, weight=1)
        content_frame.rowconfigure(0, weight=1)
        content_frame.rowconfigure(1, weight=1)
        content_frame.rowconfigure(2, weight=1)
        content_frame.rowconfigure(3, weight=1)

        try:
            # Carrega a imagem usando PIL
            original_image = Image.open('img/Raislogo.png')
            # Redimensiona a imagem (exemplo: 200x100 pixels)
            resized_image = original_image.resize((200, 100), Image.LANCZOS)
            self.logo_image = ImageTk.PhotoImage(resized_image)
            
            logo_label = ttk.Label(content_frame, image=self.logo_image)
            logo_label.grid(row=0, column=0, pady=(0, 5)) # Ajustado pady para ficar mais junto
        except FileNotFoundError:
            error_label = ttk.Label(content_frame, text="Erro: Raislogo.png não encontrada.", foreground="red")
            error_label.grid(row=0, column=0, pady=(0, 5))
        except Exception as e:
            error_label = ttk.Label(content_frame, text=f"Erro ao carregar a logo: {e}", foreground="red")
            error_label.grid(row=0, column=0, pady=(0, 5))

        info_text = "Para sugestões, melhorias e relatórios de bugs, por favor, entre em contato através do seguinte canal:" # Texto ajustado
        info_label = ttk.Label(content_frame, text=info_text, wraplength=400, justify=tk.CENTER)
        info_label.grid(row=1, column=0, pady=(5, 5)) # Ajustado pady

        email_label = ttk.Label(content_frame, text="Email: eltonjmarinho@gmail.com")
        email_label.grid(row=2, column=0, pady=(0, 0)) # Ajustado pady e removido whatsapp_label

    def _redraw_year_checkboxes(self, event=None):
        if not hasattr(self, 'years_items_frame'): return
        current_width = self.years_items_frame.winfo_width()

        if event is not None and (current_width == self._years_last_width or current_width < 5):
            return
        
        self._years_last_width = current_width

        for widget in self.years_items_frame.winfo_children(): widget.destroy()

        checkbox_width = 90
        num_columns = max(1, current_width // checkbox_width)

        for i, (year, var) in enumerate(self.year_vars.items()):
            cb = ttk.Checkbutton(self.years_items_frame, text=year, variable=var, command=self._update_file_list)
            row, col = divmod(i, num_columns)
            cb.grid(row=row, column=col, sticky=tk.W, padx=3)

    def _update_file_list(self):
        for widget in self.scrollable_files_frame.scrollable_frame.winfo_children(): widget.destroy()
        self.file_vars.clear()

        selected_years = [year for year, var in self.year_vars.items() if var.get()]
        files_to_show = set()
        for year in selected_years:
            files_to_show.update(self.available_data.get(year, {}).get('files', []))

        if not files_to_show:
            ttk.Label(self.scrollable_files_frame.scrollable_frame, text="Selecione um ano para ver os arquivos.").pack(pady=10)
            return

        self.file_vars = {fname: tk.BooleanVar(value=True) for fname in sorted(list(files_to_show))}
        for name, var in self.file_vars.items():
            cb = ttk.Checkbutton(self.scrollable_files_frame.scrollable_frame, text=name, variable=var)
            cb.pack(anchor="w", padx=10)

    def log(self, message):
        if not hasattr(self, 'status_area'): return
        self.status_area.configure(state="normal")
        self.status_area.insert(tk.END, message + "\n")
        self.status_area.configure(state="disabled")
        self.status_area.see(tk.END)

    def _toggle_selection_widgets(self, state='disabled'):
        """Disables or enables year and file selection widgets."""
        for widget in self.years_items_frame.winfo_children():
            if isinstance(widget, ttk.Checkbutton):
                widget.config(state=state)
                
        for widget in self.scrollable_files_frame.scrollable_frame.winfo_children():
            if isinstance(widget, (ttk.Checkbutton, ttk.Label)):
                try:
                    widget.config(state=state)
                except tk.TclError:
                    pass # Some widgets might not have a 'state' option

    def _reset_ui_on_finish(self):
        """Resets the UI to the initial state after a process is finished or cancelled."""
        self.start_button.config(state="normal")
        self.cancel_button.config(state="disabled")
        self._toggle_selection_widgets('normal')
        self.overall_progress['value'] = 0
        self.file_progress['value'] = 0
        self.current_file_label.config(text="Arquivo: N/A")
        self.size_label.config(text="Tamanho: N/A")
        self.speed_label.config(text="Velocidade: N/A")
        self.overall_progress_label.config(text="Progresso Total: N/A")

    def _on_tab_change(self, event):
        selected_tab_id = self.notebook.select()
        selected_tab_text = self.notebook.tab(selected_tab_id, "text")
        
        if selected_tab_text == "Exportação de Dados":
            self._refresh_extracted_files_list()

    def process_queue(self):
        try:
            message = self.queue.get_nowait()
            msg_type, value = message
            
            if msg_type == "TOTAL_PROGRESS_MAX":
                self.overall_progress['maximum'] = value
                self.overall_progress_label.config(text=f"Progresso Total: 0 / {value}")
            elif msg_type == "TOTAL_PROGRESS_UPDATE":
                self.overall_progress['value'] = value
                self.overall_progress_label.config(text=f"Progresso Total: {value} / {self.overall_progress['maximum']}")
            elif msg_type == "FILE_PROGRESS_START":
                self.current_file_label.config(text=f"Arquivo: {value['file']}")
                self.file_progress['maximum'] = value['total_size']
                self.file_progress['value'] = 0
                self.size_label.config(text=f"Tamanho: 0 KB / {self._format_bytes(value['total_size'])}")
                self.speed_label.config(text="Velocidade: N/A")
                self.current_file_total_size = value['total_size']
                self.current_file_bytes_downloaded = 0
                self.current_file_start_time = time.time()
                self.last_update_time = time.time()
                self.last_update_bytes = 0
            elif msg_type == "FILE_PROGRESS_UPDATE":
                file_name = value['file']
                bytes_downloaded = value['bytes_downloaded']
                
                self.file_progress['value'] = bytes_downloaded
                
                current_time = time.time()
                time_diff = current_time - self.last_update_time
                bytes_diff = bytes_downloaded - self.last_update_bytes

                if time_diff > 0: 
                    speed_bps = bytes_diff / time_diff
                    self.speed_label.config(text=f"Velocidade: {self._format_bytes(speed_bps)}/s")
                
                self.size_label.config(text=f"Tamanho: {self._format_bytes(bytes_downloaded)} / {self._format_bytes(self.current_file_total_size)}")
                
                self.last_update_time = current_time
                self.last_update_bytes = bytes_downloaded

            elif msg_type == "LOG":
                self.log(value)
                if value == "Processo interrompido.":
                    self._reset_ui_on_finish()
            elif msg_type == "DONE":
                self.log("Processo finalizado!")
                self._reset_ui_on_finish()
        except Empty:
            pass
        except Exception as e:
            self.log(f"[ERROR] Exceção em process_queue: {e}")
        self.root.after(100, self.process_queue)

    def _request_cancel(self):
        if messagebox.askyesno("Cancelar Processo", "Tem certeza que deseja cancelar o processo atual? "): 
            self.log("Cancelamento solicitado pelo usuário...")
            self.cancel_button.config(state="disabled")
            self.download_manager.cancel_active_downloads()

    def _start_processing_thread(self):
        selected_years = [year for year, var in self.year_vars.items() if var.get()]
        selected_files = [fname for fname, var in self.file_vars.items() if var.get()]
        
        if not selected_years or not selected_files:
            self.log("Erro: Selecione pelo menos um ano e um arquivo."); return

        self.start_button.config(state="disabled")
        self.cancel_button.config(state="normal")
        self._toggle_selection_widgets('disabled')
        self.overall_progress['value'] = 0
        self.file_progress['value'] = 0
        self.status_area.configure(state="normal"); self.status_area.delete(1.0, tk.END); self.status_area.configure(state="disabled")
        self.log("Iniciando processo...")
        
        worker_thread = threading.Thread(
            target=self.download_manager.start_processing,
            args=(selected_years, selected_files, self.available_data),
            daemon=True
        )
        worker_thread.start()
