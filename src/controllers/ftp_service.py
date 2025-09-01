import ftplib
import re

class FTPService:
    """Encapsula a lógica de interação com o servidor FTP."""
    FTP_HOST = "ftp.mtps.gov.br"
    FTP_PATH = "/pdet/microdados/RAIS/"

    def __init__(self, queue):
        self.queue = queue

    def fetch_available_data(self):
        """Busca a lista de anos e arquivos disponíveis no FTP."""
        try:
            self.queue.put(("LOG", "Conectando ao servidor FTP..."))
            ftp = ftplib.FTP(self.FTP_HOST, timeout=30)
            ftp.login()
            self.queue.put(("LOG", "Obtendo lista de diretórios..."))
            ftp.cwd(self.FTP_PATH)
            
            all_dirs = ftp.nlst()
            dados_completos = {}
            
            for dir_name in all_dirs:
                match = re.search(r'(\d{4})', dir_name)
                if match:
                    ano = match.group(1)
                    self.queue.put(("LOG", f"Buscando arquivos para o ano {ano}..."))
                    try:
                        ftp.cwd(f"{self.FTP_PATH}/{dir_name}/")
                        arquivos_ano = [f for f in ftp.nlst() if f.lower().endswith('.7z')]
                        if arquivos_ano:
                            if ano not in dados_completos:
                                dados_completos[ano] = {'dir': dir_name, 'files': []}
                            dados_completos[ano]['files'].extend(arquivos_ano)
                    except ftplib.error_perm:
                        self.queue.put(("LOG", f"Aviso: Não foi possível acessar o diretório {dir_name}."))
                        continue
            
            sorted_dados = dict(sorted(dados_completos.items(), reverse=True))
            ftp.quit()
            self.queue.put(("FETCH_COMPLETE", sorted_dados))
        except Exception as e:
            self.queue.put(("LOG", f"Erro Crítico ao buscar dados do FTP: {e}"))
            self.queue.put(("FETCH_COMPLETE", {}))
