import { ClassificacaoCuidado } from "../entities/AvaliacaoSCP";

/** Horas/paciente/dia por classe (defaults baseados na sua planilha) */
export const HORAS_PADRAO: Record<ClassificacaoCuidado, number> = {
  [ClassificacaoCuidado.MINIMOS]: 4, // PCM
  [ClassificacaoCuidado.INTERMEDIARIOS]: 6, // PCI
  [ClassificacaoCuidado.ALTA_DEPENDENCIA]: 10, // PADC
  [ClassificacaoCuidado.SEMI_INTENSIVOS]: 10, // PCSI
  [ClassificacaoCuidado.INTENSIVOS]: 18, // PCIt
};

export type DistPorClasse = Partial<Record<ClassificacaoCuidado, number>>;

export interface ParametrosDimensionamento {
  /** fração da equipe que é enfermeiro (0..1). Tec = 1 - pctEnf */
  pctEnf?: number; // default 0.33
  pctTec?: number; // default 1 - pctEnf
  /** aplicar IST (índice de segurança técnica)? */
  aplicarIST?: boolean; // default false
  /** valor do IST (ex.: 0.15 = 15%) */
  ist?: number; // default 0.15
  /** constante de Marinho (por categoria) */
  kmEnf?: number; // default 0.2236
  kmTec?: number; // default 0.2236
  /** horas por classe (para permitir ajustes locais) */
  horasPorClasse?: Record<ClassificacaoCuidado, number>;
}

/** Resultado do cálculo para apresentação */
export interface ResultadoDimensionamento {
  the: number; // Total de Horas de Enfermagem (por dia médio)
  distribuicaoMediaDia: DistPorClasse;
  horasPorClasseUsadas: Record<ClassificacaoCuidado, number>;
  pctEnf: number;
  pctTec: number;
  kmEnf: number;
  kmTec: number;
  istAplicado: number; // 0 ou valor (ex.: 0.15)
  qpEnf: { decimal: number; arredondado: number };
  qpTec: { decimal: number; arredondado: number };
}

/**
 * Calcula THE e QP (enf/tec) a partir da distribuição média diária por classe.
 * Fórmula modelo (quando IST não aplicado):
 *   THE = Σ( qtdClasse * horasClasse )
 *   QP_enf = THE * KM_enf * pctEnf
 *   QP_tec = THE * KM_tec * pctTec
 * Se aplicar IST: multiplicar QP por (1 + IST).
 */
export function calcularDimensionamento(
  distribuicaoMediaDia: DistPorClasse,
  params: ParametrosDimensionamento = {}
): ResultadoDimensionamento {
  const horas = params.horasPorClasse ?? HORAS_PADRAO;
  const pctEnf = params.pctEnf ?? 0.33;
  const pctTec = 1 - pctEnf;
  const kmEnf = params.kmEnf ?? 0.2236;
  const kmTec = params.kmTec ?? 0.2236;
  const istValor = params.aplicarIST ? params.ist ?? 0.15 : 0;

  let THE = 0;
  (Object.keys(horas) as Array<keyof typeof horas>).forEach((k) => {
    const qtd = Number(distribuicaoMediaDia[k] ?? 0);
    THE += qtd * horas[k];
  });

  let qpEnf = THE * kmEnf * pctEnf;
  let qpTec = THE * kmTec * pctTec;

  if (istValor > 0) {
    qpEnf *= 1 + istValor;
    qpTec *= 1 + istValor;
  }

  return {
    the: THE,
    distribuicaoMediaDia,
    horasPorClasseUsadas: horas,
    pctEnf,
    pctTec,
    kmEnf,
    kmTec,
    istAplicado: istValor,
    qpEnf: { decimal: qpEnf, arredondado: Math.ceil(qpEnf) },
    qpTec: { decimal: qpTec, arredondado: Math.ceil(qpTec) },
  };
}
