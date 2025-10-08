import { ClassificacaoCuidado } from "../entities/AvaliacaoSCP";
import { SCPType } from "./scpSchemas";

// Faixas por SCP. Ajuste FUGULIN conforme o protocolo adotado no hospital.
export const faixasPorSCP: Record<
  SCPType,
  Array<{ min: number; max: number; classe: ClassificacaoCuidado }>
> = {
  PERROCA: [
    { min: 13, max: 26, classe: ClassificacaoCuidado.MINIMOS },
    { min: 27, max: 39, classe: ClassificacaoCuidado.INTERMEDIARIOS },
    { min: 40, max: 52, classe: ClassificacaoCuidado.SEMI_INTENSIVOS },
    { min: 53, max: 65, classe: ClassificacaoCuidado.INTENSIVOS },
  ],
  DINI: [
    { min: 11, max: 17, classe: ClassificacaoCuidado.MINIMOS },
    { min: 18, max: 23, classe: ClassificacaoCuidado.INTERMEDIARIOS },
    { min: 24, max: 30, classe: ClassificacaoCuidado.ALTA_DEPENDENCIA },
    { min: 31, max: 36, classe: ClassificacaoCuidado.SEMI_INTENSIVOS },
    { min: 37, max: 9999, classe: ClassificacaoCuidado.INTENSIVOS },
  ],
  // Valores de exemplo para FUGULIN — substitua pelos oficiais do seu protocolo
  FUGULIN: [
    { min: 0, max: 14, classe: ClassificacaoCuidado.MINIMOS },
    { min: 15, max: 24, classe: ClassificacaoCuidado.INTERMEDIARIOS },
    { min: 25, max: 28, classe: ClassificacaoCuidado.ALTA_DEPENDENCIA },
    { min: 29, max: 34, classe: ClassificacaoCuidado.SEMI_INTENSIVOS }, // visto no material: 29-34 semi
    { min: 35, max: 9999, classe: ClassificacaoCuidado.INTENSIVOS }, // visto no material: >34 intensivos
  ],
};

export function classificarTotal(
  scp: SCPType,
  total: number
): ClassificacaoCuidado {
  const faixa = faixasPorSCP[scp].find((f) => total >= f.min && total <= f.max);
  if (!faixa)
    throw new Error(`Total ${total} fora das faixas configuradas para ${scp}`);
  return faixa.classe;
}
