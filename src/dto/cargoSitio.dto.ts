export interface CreateCargoSitioDTO {
  cargoUnidadeId: string;
  sitioId: string;
  quantidade_funcionarios: number;
}

export interface UpdateCargoSitioDTO {
  quantidade_funcionarios?: number;
  cargoUnidadeId?: string;
}

export interface CargoSitioDTO {
  id: string;
  cargoUnidadeId: string;
  sitioId: string;
  quantidade_funcionarios: number;
}
