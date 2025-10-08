import {
  CreateCargoUnidadeDTO,
  CargoUnidadeResponseDTO,
} from "./cargoUnidade.dto";

export interface CreateUnidadeDTO {
  hospitalId: string;
  nome: string;
  scpMetodoId?: string;
  numeroLeitos: number;
  horas_extra_reais?: string;
  horas_extra_projetadas?: string;
  cargos_unidade?: CreateCargoUnidadeDTO[];
}

export interface UpdateUnidadeDTO {
  nome?: string;
  numeroLeitos?: number;
  scpMetodoId?: string;
  horas_extra_reais?: string;
  horas_extra_projetadas?: string;
  cargos_unidade?: CreateCargoUnidadeDTO[];
}

export interface UnidadeListDTO {
  id: string;
  nome: string;
  hospitalId: string | null;
  scpMetodoKey: string | null;
  scpMetodoId: string | null;
  horas_extra_reais?: string;
  horas_extra_projetadas?: string;
  leitos: any[];
  cargos_unidade?: CargoUnidadeResponseDTO[];
  created_at: Date;
  updated_at: Date;
}
