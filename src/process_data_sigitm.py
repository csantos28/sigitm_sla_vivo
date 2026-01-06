from pathlib import Path
from platformdirs import user_downloads_dir
import warnings
import pandas as pd
from datetime import datetime
import pytz
from typing import Optional
from dataclasses import dataclass
from .syslog import SystemLogger

@dataclass
class FileProcessingResult:
    success: bool
    message: str
    dataframe: Optional[pd.DataFrame] = None

class ExcelFileHandler:
    """Handler para processamento de arquivos Excel com prefixo específico.
    
    Attributes:
        directory (Path): Diretório para busca dos arquivos
        prefix (str): Prefixo dos arquivos a serem processados
        column_mapping (Dict[str, str]): Mapeamento de colunas para renomeação
        date_columns (Tuple[str, ...]): Colunas que devem ser tratadas como datas
    """

    COLUMN_MAPPING = {
        'Sequencia': 'sequencia',
        'Raiz': 'raiz',
        'Empresa Manutenção': 'empresa_manutencao',        
        'Tipo de Alarme': 'tipo_de_alarme',
        'Tipo de Bilhete': 'tipo_de_bilhete',
        'Tipo de Falha': 'tipo_de_falha',
        'Data Criacao': 'data_criacao',
        'Data Encerramento': 'data_encerramento',
        'Sigla Estado': 'sigla_estado',
        'Nome Estado': 'nome_estado',
        'Nome Localidade': 'nome_localidade',
        'Codigo Gerencia': 'codigo_gerencia',
        'Nome Gerencia': 'nome_gerencia',
        'Nome Município': 'nome_municipio',        
        'Nome Area': 'nome_area',
        'Grupo Responsavel': 'grupo_responsavel',
        'Grupo Criador': 'grupo_criador',
        'Tipo Rede': 'tipo_rede',
        'Baixado por Grupo': 'baixado_por_grupo',
        'Código Baixa': 'codigo_baixa', 
        'Baixa Grupo': 'baixa_grupo',
        'Baixa Componente': 'baixa_componente',         
        'Baixa Órgão': 'baixa_orgao',
        'Baixa Causa': 'baixa_causa',
        'Baixa Reparo': 'baixa_reparo',
        'Baixa Defeito': 'baixa_defeito',
        'Sigla Localidade': 'sigla_localidade',        
        'Código Area': 'codigo_area',
        'Sigla Localidade Dest Optica': 'sigla_localidade_dest_optica',
        'Codigo Area Dest Optica': 'codigo_area_dest_optica',
        'Endereço': 'endereco',
        'Bairro': 'bairro',
        'Endereço falha Óptica': 'endereco_falha_optica',
        'VTA PK': 'vta_pk' 
    }

    PREFIX = "CONSULTA_LOTE4_FECHADAS"

    DATE_COLUMNS = ("data_criacao", "data_encerramento")
    DISPLAY_DATETIME_FORMAT = "%Y-%m-%d %H:%M"

    def __init__(self, directory: Optional[Path] = None, prefix: str = PREFIX):
        """Inicializa o handler com diretório e prefixo.
        
        Args:
            directory: Diretório para busca. Padrão: diretório de downloads do usuário
            prefix: Prefixo dos arquivos a serem processados
        """

        self.directory = Path(directory) if directory else Path(user_downloads_dir())
        self.prefix = prefix
        self.logger = SystemLogger.configure_logger("ExcelFileHandler")

        warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

        if not self.directory.exists():
            self.logger.warning(f"❌ Diretório não encontrado: {self.directory}")
            self.directory.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"✅ Diretório criado: {self.directory}")
    
    def _find_most_recent_file(self) -> Optional[Path]:
        """Encontra o arquivo mais recente com o prefixo configurado.
        
        Returns:
            Path do arquivo mais recente ou None se não encontrado
            
        Raises:
            FileNotFoundError: Se nenhum arquivo for encontrado
        """

        search_pattern = f"{self.prefix}*"
        files = list(self.directory.glob(search_pattern))

        if not files:
            self.logger.error(f"❌ Nenhum arquivo encontrado com o prefixo: {self.prefix}")
            raise FileNotFoundError(f"❌ Nenhum arquivo com prefixo {self.prefix} encontrado em {self.directory}")
        
        return max(files, key=lambda f: f.stat().st_mtime)
    
    def _process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Processa o dataframe com transformações necessárias.
        
        Args:
            df: DataFrame original
            file_path: Path do arquivo para extrair metadados
            
        Returns:
            DataFrame processado
        """
        # Renomeia colunas
        df = df.rename(columns=self.COLUMN_MAPPING)

        # Defini fuso horário do Brasil(BRT - Brasília, UTC-3)
        fuso_horario = pytz.timezone("America/Sao_Paulo")

        # Pegando a data atual, e concatenando com a hora 00:00
        dthr_corte = datetime.now(fuso_horario).replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=None)

        for col in self.DATE_COLUMNS:
            if col in df.columns:
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter('ignore', UserWarning)
                        # 1. Converte a string original para objeto datetime do Pandas
                        df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

                        # 2. Remove fuso horário da coluna caso o Pandas tenha inserido
                        if df[col].dt.tz is not None:
                            df[col] = df[col].dt.tz_localize(None)
                
                except Exception as e:
                    self.logger.warning(f"⚠️ Erro no processamento da coluna {col}: {e}")

        df = df[df['data_encerramento'] < dthr_corte].copy()

        # Formatação final para string
        for col in self.DATE_COLUMNS:
            if col in df.columns:
                df[col] = df[col].dt.strftime(self.DISPLAY_DATETIME_FORMAT).where(df[col].notnull(), None)               

        # Tratamento de IDs e tipagem Segura                        
        id_cols = ["sequencia", "vta_pk", "raiz"]

        for col in id_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype('Int64').astype(str).replace('0', None)
        
        # Limpeza Final (Substitui 'NaT' gerado pelo strftime por None)
        df = df.replace({pd.NA: None, "nan": None, "None": None, "": None, "NaT": None})
        
        # Normalização de colunas de texto
        text_cols = df.select_dtypes(include=['object']).columns
        df[text_cols] = df[text_cols].astype(str).replace("None", None)

        # Deleta coluna VTA PK
        df.drop('vta_pk', axis=1, inplace=True)                

        return df   

    def _load_to_dataframe(self, file_path: Path) -> FileProcessingResult:
        """Carrega arquivos Excel """
        try:

            df = pd.read_excel(file_path, engine='calamine')

            processed_df = self._process_dataframe(df)

            self.logger.info("✅ Arquivo Excel processado com sucesso.")
            
            return FileProcessingResult(success=True, message="Arquivo processado com sucesso", dataframe=processed_df)
        
        except Exception as e:
            return FileProcessingResult(success=False, message=f"Erro ao processar arquivo: {str(e)}") 

    def process_most_recent_file(self, file_path: Path) -> FileProcessingResult:
        """Processa o arquivo mais recente encontrado.
        
        Returns:
            FileProcessingResult com status e dados
        """
        try:
            # Se o orquestrador já possui o caminho, usa ele. Do contrário, busca no disco (fallback).
            target_path = file_path if file_path else self._find_most_recent_file()

            self.logger.info(f"🎯 Alvo de processamento: {target_path.name}")
            return self._load_to_dataframe(target_path)

        except Exception as e:
            self.logger.error(f"❌ Erro ao processar arquivo mais recente: {e}")
            return FileProcessingResult(success=False, message=f"Erro ao processar arquivo mais recente: {str(e)}")

    def delete_most_recent_file(self, file_path: Path) -> bool:
        """Remove o arquivo mais recente encontrado.
        
        Returns:
            bool: True se removido com sucesso, False caso contrário
        """
        try:
            # Se o orquestrador já possui o caminho, usa ele. Do contrário, busca no disco (fallback).
            target_path = file_path if file_path else self._find_most_recent_file()
            target_path.unlink()
            self.logger.info(f"✅ Arquivo removido com sucesso: {file_path}")
            return True

        except Exception as e:
            self.logger.error(f"❌ Erro ao remover arquivo: {e}")
            return False                                   