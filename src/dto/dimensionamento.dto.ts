// Estrutura de uma linha da tabela de análise, usada em ambas as respostas
export interface LinhaAnaliseFinanceira {
  cargoId: string;
  cargoNome: string;
  isScpCargo: boolean;
  salario: number;
  adicionais: number;
  valorHorasExtras: number;
  custoPorFuncionario: number;
  cargaHoraria: number;
  quantidadeAtual: number;
  quantidadeProjetada: number;
}

// Resposta para Unidade de Internação
export interface AnaliseInternacaoResponse {
  agregados: {
    periodo: {
      inicio: string;
      fim: string;
      dias: number;
    };
    totalLeitosDia: number;
    totalAvaliacoes: number;
    taxaOcupacaoMensal: number;
    distribuicaoTotalClassificacao?: { [key: string]: number }; // Adicionado para o frontend
  };
  tabela: LinhaAnaliseFinanceira[];
}

// Estrutura para agrupar cargos por sítio na resposta de não internação
export interface GrupoCargosNaoInternacao {
  id: string; // ID do Sítio
  nome: string; // Nome do Sítio
  cargos: LinhaAnaliseFinanceira[];
}

export interface ResumoDistribuicaoNaoInternacao {
  porSitio: Array<{
    sitioId: string;
    sitioNome?: string;
    categoria: "ENF" | "TEC";
    totalSemana: number;
    totalFimSemana: number;
    total: number;
  }>;
  totais: {
    enfermeiro: number;
    tecnico: number;
  };
}

export interface ResumoDimensionamentoNaoInternacao {
  periodoTrabalho: number;
  kmEnfermeiro: number;
  kmTecnico: number;
  totalSitiosEnfermeiro: number;
  totalSitiosTecnico: number;
  pessoalEnfermeiro: number;
  pessoalTecnico: number;
  pessoalEnfermeiroArredondado: number;
  pessoalTecnicoArredondado: number;
}

// Resposta para Unidade de Não Internação
export interface AnaliseNaoInternacaoResponse {
  tabela: GrupoCargosNaoInternacao[];
  horasExtrasProjetadas: number;
  parametros?: {
    jornadaSemanalEnfermeiro?: number;
    jornadaSemanalTecnico?: number;
    indiceSegurancaTecnica: number;
    equipeComRestricao: boolean;
    diasFuncionamentoMensal: number;
    diasSemana: number;
    periodoTrabalho: number;
  };
  distribuicao?: ResumoDistribuicaoNaoInternacao;
  dimensionamento?: ResumoDimensionamentoNaoInternacao;
}
