export interface UnidadeNaoInternacaoDTO {
  id: string;
  hospitalId: string;
  nome: string;
  descricao?: string;
  horas_extra_reais?: string;
  horas_extra_projetadas?: string;
}

export interface CriarUnidadeNaoInternacaoDTO {
  hospitalId: string;
  nome: string;
  descricao?: string;
  horas_extra_reais?: string;
  horas_extra_projetadas?: string;
  sitios_funcionais?: {
    nome?: string;
    descricao?: string;
    especificacoes?: any;
    tempo_padrao_procedimento?: number;
  }[];
  cargos_unidade?: {
    cargoId: string;
    quantidade_funcionarios: number;
  }[];
}

export interface AtualizarUnidadeNaoInternacaoDTO {
  nome?: string;
  descricao?: string;
  horas_extra_reais?: string;
  horas_extra_projetadas?: string;
  sitios_funcionais?: {
    nome?: string;
    descricao?: string;
    especificacoes?: any;
    tempo_padrao_procedimento?: number;
  }[];
  cargos_unidade?: {
    cargoId: string;
    quantidade_funcionarios: number;
  }[];
}
