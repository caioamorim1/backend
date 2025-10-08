interface SetorBaseline {
  nome: string;
  custo: string;
  ativo: boolean;
}
export interface CriarBaselineDTO {
  hospitalId: string;
  nome: string;
  quantidade_funcionarios?: number;
  custo_total?: string;
  setores?: SetorBaseline[];
}

export interface AtualizarBaselineDTO {
  nome?: string;
  quantidade_funcionarios?: number;
  custo_total?: string;
  setores?: SetorBaseline[];
}
