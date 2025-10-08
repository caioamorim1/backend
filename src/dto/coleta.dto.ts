import { Colaborador } from "../entities/Colaborador";

export interface RespostaColetaDTO {
  perguntaId: string;
  valor: any;
  comentario?: string;
  fotoUrl?: string;
}

export interface CriarColetaDTO {
  questionarioId: string;
  unidadeId?: string;
  sitioId?: string;
  localNome: string;
  respostas: RespostaColetaDTO[];
  colaboradorId?: string;
  colaborador?: Colaborador;
}

export interface ColetaDTO {
  id: string;
  questionarioId: string;
  unidadeId?: string;
  sitioId?: string;
  localNome: string;
  respostas: RespostaColetaDTO[];
  created_at: string;
  updated_at: string;
}
