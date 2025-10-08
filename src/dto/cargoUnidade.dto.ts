export interface CreateCargoUnidadeDTO {
  cargoId: string;
  quantidade_funcionarios: number;
}

export interface UpdateCargoUnidadeDTO {
  cargoId?: string;
  quantidade_funcionarios?: number;
}

export interface CargoUnidadeResponseDTO {
  id: string;
  cargoId: string;
  quantidade_funcionarios: number;
  cargo: {
    id: string;
    nome: string;
    salario?: string;
    carga_horaria?: string;
    descricao?: string;
    adicionais_tributos?: string;
  };
}
