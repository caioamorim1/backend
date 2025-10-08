import { ClassificacaoCuidado } from "../entities/AvaliacaoSCP";

export class CreateAvaliacaoSCPDTO {
  dataAplicacao!: string; // yyyy-mm-dd
  unidadeId?: string;
  internacaoId!: string;
  colaboradorId!: string;
  scp!: string;
  itens!: Record<string, number>;
  totalPontos!: number;
  classificacao!: string;
}

export interface FiltroAvaliacaoDTO {
  data: string; // yyyy-mm-dd
  unidadeId: string;
}

export interface ResumoDiarioDTO {
  data: string;
  unidadeId: string;
  totalOcupados: number;
  numeroLeitos: number;
  taxaOcupacao: number; // 0..1
  distribuicao: Partial<Record<ClassificacaoCuidado, number>>;
}
