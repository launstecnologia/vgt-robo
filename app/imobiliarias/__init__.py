from .addad import IMOBILIARIA as ADDAD
from .agnello import IMOBILIARIA as AGNELLO
from .alianzzo import IMOBILIARIA as ALIANZZO
from .arbix import IMOBILIARIA as ARBIX
from .ativa import IMOBILIARIA as ATIVA
from .bg import IMOBILIARIA as BG
from .bons_negocios import IMOBILIARIA as BONS_NEGOCIOS
from .casa_grande import IMOBILIARIA as CASA_GRANDE
from .cohab import IMOBILIARIA as COHAB
from .coliseu import IMOBILIARIA as COLISEU
from .compacto import IMOBILIARIA as COMPACTO
from .concreto import IMOBILIARIA as CONCRETO
from .correta import IMOBILIARIA as CORRETA
from .estrutura import IMOBILIARIA as ESTRUTURA
from .fg import IMOBILIARIA as FG
from .franca import IMOBILIARIA as FRANCA
from .hmpolo import IMOBILIARIA as HMPOLO
from .imobilar import IMOBILIARIA as IMOBILAR
from .imovan import IMOBILIARIA as IMOVAN
from .justo import IMOBILIARIA as JUSTO
from .lago import IMOBILIARIA as LAGO
from .redentora import IMOBILIARIA as REDENTORA
from .locabens import IMOBILIARIA as LOCABENS
from .lpg import IMOBILIARIA as LPG
from .maciel import IMOBILIARIA as MACIEL
from .malufi import IMOBILIARIA as MALUFI
from .martins import IMOBILIARIA as MARTINS
from .mbrokers import IMOBILIARIA as MBROKERS
from .mediterraneo import IMOBILIARIA as MEDITERRANEO
from .mybroker_rp import IMOBILIARIA as MYBROKER_RP
from .mybroker_uberlandia import IMOBILIARIA as MYBROKER_UBERLANDIA
from .pedro_granado import IMOBILIARIA as PEDRO_GRANADO
from .phercon import IMOBILIARIA as PHERCON
from .pratica import IMOBILIARIA as PRATICA
from .procurello_bia_marques import IMOBILIARIA as PROCURELLO_BIA_MARQUES
from .resende import IMOBILIARIA as RESENDE
from .s_a import IMOBILIARIA as S_A
from .sjc import IMOBILIARIA as SJC
from .sol import IMOBILIARIA as SOL
from .stela import IMOBILIARIA as STELA
from .tecond import IMOBILIARIA as TECOND
from .teixeira import IMOBILIARIA as TEIXEIRA
from .valor import IMOBILIARIA as VALOR
from .vania import IMOBILIARIA as VANIA
from .cavalo_marinho import IMOBILIARIA as CAVALO_MARINHO
from .wuo import IMOBILIARIA as WUO
from ..segredos_loader import aplicar_segredos_imobiliarias

IMOBILIARIAS_EVENTOS = [
    ADDAD,
    AGNELLO,
    ALIANZZO,
    ARBIX,
    ATIVA,
    BG,
    BONS_NEGOCIOS,
    CASA_GRANDE,
    COHAB,
    COLISEU,
    COMPACTO,
    CONCRETO,
    CORRETA,
    ESTRUTURA,
    FG,
    FRANCA,
    HMPOLO,
    IMOBILAR,
    IMOVAN,
    JUSTO,
    LAGO,
    REDENTORA,
    LOCABENS,
    LPG,
    MACIEL,
    MALUFI,
    MARTINS,
    MBROKERS,
    MEDITERRANEO,
    MYBROKER_RP,
    MYBROKER_UBERLANDIA,
    PEDRO_GRANADO,
    PHERCON,
    PRATICA,
    PROCURELLO_BIA_MARQUES,
    RESENDE,
    S_A,
    SJC,
    SOL,
    STELA,
    TECOND,
    TEIXEIRA,
    VALOR,
    VANIA,
    CAVALO_MARINHO,
    WUO,
]

IMOBILIARIAS_EVENTOS = aplicar_segredos_imobiliarias(IMOBILIARIAS_EVENTOS)
