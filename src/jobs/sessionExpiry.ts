import { DataSource } from "typeorm";
import { DateTime } from "luxon";
import { AvaliacaoSCP, StatusSessaoAvaliacao } from "../entities/AvaliacaoSCP";
import { HistoricoOcupacao } from "../entities/HistoricoOcupacao";
import { Leito, StatusLeito } from "../entities/Leito";

/**
 * Agendamento diário para:
 *  - salvar histórico de ocupação das avaliações do dia anterior
 *  - resetar status dos leitos ocupados para VAGO (preservando manutenção)
 *  - marcar sessões como LIBERADA
 *
 * Observações/assunções:
 *  - rodamos no primeiro instante do dia (meia-noite) e processamos as avaliações
 *    cujo `dataAplicacao` corresponde ao dia anterior (UTC, mesmo formato yyyy-mm-dd usado no app)
 *  - não sobrescrevemos leitos em manutenção (MANUT_*)
 *  - armazenamos `prontuario` em `pacienteNome` do histórico quando disponível
 */
export async function runSessionExpiryForDate(
  ds: DataSource,
  dateYYYYMMDD: string
) {
  const ZONE = "America/Sao_Paulo";
  try {
    const avalRepo = ds.getRepository(AvaliacaoSCP);
    const histRepo = ds.getRepository(HistoricoOcupacao);
    const leitoRepo = ds.getRepository(Leito);

    const avals = await avalRepo.find({
      where: { dataAplicacao: dateYYYYMMDD },
      relations: ["leito", "unidade", "unidade.hospital", "autor"],
    });

    const historicos: Partial<HistoricoOcupacao>[] = [];
    const leitoIdsToReset = new Set<string>();

    for (const a of avals) {
      if (!a.leito) continue;
      leitoIdsToReset.add(a.leito.id);

      const startLocal = DateTime.fromISO(dateYYYYMMDD, { zone: ZONE }).startOf(
        "day"
      );
      const endLocal = startLocal.endOf("day");

      const h: Partial<HistoricoOcupacao> = {
        leito: a.leito as any,
        unidadeId: a.unidade ? (a.unidade as any).id : null,
        hospitalId:
          a.unidade && (a.unidade as any).hospital
            ? (a.unidade as any).hospital.id
            : null,
        leitoNumero: a.leito.numero ?? null,
        leitoStatus: (a.leito as any).status ?? null,
        scp: a.scp ?? null,
        totalPontos: a.totalPontos ?? null,
        classificacao: a.classificacao ?? null,
        itens: a.itens ?? null,
        autorId: a.autor ? (a.autor as any).id : null,
        autorNome: a.autor ? (a.autor as any).nome : null,
        inicio: startLocal.toUTC().toJSDate(),
        fim: endLocal.toUTC().toJSDate(),
      };

      historicos.push(h);
    }

    if (historicos.length) {
      for (const h of historicos) {
        const ent = histRepo.create(h as any);
        // eslint-disable-next-line no-await-in-loop
        await histRepo.save(ent);
      }
    }

    // Resetar TODOS os leitos para PENDENTE (independente de ter avaliação)
    await leitoRepo
      .createQueryBuilder()
      .update(Leito)
      .set({ status: StatusLeito.PENDENTE })
      .execute();

    await avalRepo
      .createQueryBuilder()
      .update(AvaliacaoSCP)
      .set({ statusSessao: StatusSessaoAvaliacao.EXPIRADA })
      .where('"dataAplicacao" = :d', { d: dateYYYYMMDD })
      .execute();
  } catch (e) {
    console.warn("Job manual run failed:", e);
    throw e;
  }
}

export function scheduleSessionExpiry(ds: DataSource) {
  const ZONE = "America/Sao_Paulo";
  const MS_PER_DAY = 24 * 60 * 60 * 1000; // only for fallback / intervals between calculations

  const runForDate = async (dateYYYYMMDD: string) => {
    try {
      const avalRepo = ds.getRepository(AvaliacaoSCP);
      const histRepo = ds.getRepository(HistoricoOcupacao);
      const leitoRepo = ds.getRepository(Leito);

      // busca avaliações do dia (ex.: ontem)
      const avals = await avalRepo.find({
        where: { dataAplicacao: dateYYYYMMDD },
        relations: ["leito", "unidade", "unidade.hospital", "autor"],
      });

      // cria historico para cada avaliação que tiver leito associado
      const historicos: Partial<HistoricoOcupacao>[] = [];
      const leitoIdsToReset = new Set<string>();

      for (const a of avals) {
        if (!a.leito) continue;
        leitoIdsToReset.add(a.leito.id);

        // inicio/fim representados em UTC, mas cobrindo o dia local (São Paulo) completo
        const startLocal = DateTime.fromISO(dateYYYYMMDD, {
          zone: ZONE,
        }).startOf("day");
        const endLocal = startLocal.endOf("day");

        const h: Partial<HistoricoOcupacao> = {
          leito: a.leito as any,
          unidadeId: a.unidade ? (a.unidade as any).id : null,
          hospitalId:
            a.unidade && (a.unidade as any).hospital
              ? (a.unidade as any).hospital.id
              : null,
          leitoNumero: a.leito.numero ?? null,
          // snapshot do status do leito
          leitoStatus: (a.leito as any).status ?? null,
          // Dados da avaliação
          scp: a.scp ?? null,
          totalPontos: a.totalPontos ?? null,
          classificacao: a.classificacao ?? null,
          itens: a.itens ?? null,
          autorId: a.autor ? (a.autor as any).id : null,
          autorNome: a.autor ? (a.autor as any).nome : null,
          // janela completa daquele dia na timezone de SP convertida para UTC
          inicio: startLocal.toUTC().toJSDate(),
          fim: endLocal.toUTC().toJSDate(),
        };

        historicos.push(h);
      }

      if (historicos.length) {
        // salvar individualmente para evitar confusão de tipos no overload do save()
        for (const h of historicos) {
          const ent = histRepo.create(h as any);
          // salva cada um (pode ser otimizado em batch se necessário)
          // eslint-disable-next-line no-await-in-loop
          await histRepo.save(ent);
        }
      }

      // Resetar TODOS os leitos para PENDENTE
      await leitoRepo
        .createQueryBuilder()
        .update(Leito)
        .set({ status: StatusLeito.PENDENTE })
        .execute();

      // marca sessões daquele dia como EXPIRADA (fim do dia, não alta hospitalar)
      await avalRepo
        .createQueryBuilder()
        .update(AvaliacaoSCP)
        .set({ statusSessao: StatusSessaoAvaliacao.EXPIRADA })
        .where('"dataAplicacao" = :d', { d: dateYYYYMMDD })
        .execute();
    } catch (e) {
      console.warn("Job daily-reset falhou:", e);
    }
  };

  // função que agenda o próximo disparo baseado em meia-noite da timezone alvo (DST-safe)
  const scheduleNext = () => {
    const now = DateTime.now().setZone(ZONE);
    const nextMidnight = now.plus({ days: 1 }).startOf("day");
    const delay = nextMidnight.toMillis() - now.toMillis();

    const timeoutHandle = setTimeout(async () => {
      // processa o dia anterior na timezone de SP
      const yesterday = DateTime.now()
        .setZone(ZONE)
        .minus({ days: 1 })
        .startOf("day");
      const dateStr = yesterday.toISODate(); // yyyy-mm-dd
      if (dateStr) await runForDate(dateStr);
      scheduleNext(); // re-agenda para a próxima meia-noite (recalcula para lidar com DST)
    }, delay);

    // retorna função de cleanup desse timeout
    return timeoutHandle;
  };

  const firstHandle = scheduleNext();

  return () => {
    clearTimeout(firstHandle);
  };
}
