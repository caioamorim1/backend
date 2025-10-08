export interface SitioFuncionalDTO {
  id: string;
  unidadeId: string;
  nome?: string;
  descricao?: string;
  numeroPositions: number;
}

export interface CriarSitioFuncionalDTO {
  unidadeId: string;
  nome?: string;
  descricao?: string;
  numeroPositions: number;
}

export interface AtualizarSitioFuncionalDTO {
  numero?: string;
  nome?: string;
  descricao?: string;
  numeroPositions?: number;
}

// Allow providing cargos when creating/updating a sitio. Each item may
// include either `cargoUnidadeId` (preferred) or `cargoId` (frontend may
// provide this). quantidade_funcionarios is optional and defaults to 1.
export interface SitioCargoInputDTO {
  cargoUnidadeId?: string;
  cargoId?: string;
  quantidade_funcionarios?: number;
}

export interface SitioDistribuicaoInputDTO {
  categoria: "ENF" | "TEC";
  segSexManha?: number;
  segSexTarde?: number;
  segSexNoite1?: number;
  segSexNoite2?: number;
  sabDomManha?: number;
  sabDomTarde?: number;
  sabDomNoite1?: number;
  sabDomNoite2?: number;
}

export interface CriarSitioFuncionalDTOWithCargos
  extends CriarSitioFuncionalDTO {
  cargos?: SitioCargoInputDTO[];
  distribuicoes?: SitioDistribuicaoInputDTO[];
}

export interface AtualizarSitioFuncionalDTOWithCargos
  extends AtualizarSitioFuncionalDTO {
  cargos?: SitioCargoInputDTO[];
  distribuicoes?: SitioDistribuicaoInputDTO[];
}
