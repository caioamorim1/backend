import { DataSource } from "typeorm";
import { HistoricoOcupacao } from "../entities/HistoricoOcupacao";
import { DateTime } from "luxon";

export class HistoricoOcupacaoRepository {
  private ds: DataSource;
  constructor(ds: DataSource) {
    this.ds = ds;
  }

  // Lista historicos para um dia local (America/Sao_Paulo)
  async listarPorDia(data: string, unidadeId?: string) {
    // data = yyyy-mm-dd (local, SP)
    const zone = "America/Sao_Paulo";
    const startLocal = DateTime.fromISO(data, { zone }).startOf("day");
    const endLocal = startLocal.endOf("day");

    const startUtc = startLocal.toUTC().toJSDate();
    const endUtc = endLocal.toUTC().toJSDate();

    const qb = this.ds
      .getRepository(HistoricoOcupacao)
      .createQueryBuilder("h")
      .leftJoinAndSelect("h.leito", "leito")
      .where("h.inicio >= :start AND h.inicio <= :end", {
        start: startUtc,
        end: endUtc,
      });

    if (unidadeId) {
      qb.andWhere("h.unidadeId = :unidadeId", { unidadeId });
    }

    qb.orderBy("h.inicio", "ASC");

    return qb.getMany();
  }

  // Lista historicos em período [dataIni, dataFim] (iso dates yyyy-mm-dd) interpretados como dias em SP
  async listarPorPeriodo(
    dataIni: string,
    dataFim: string,
    unidadeId?: string,
    hospitalId?: string
  ) {
    const zone = "America/Sao_Paulo";
    const startLocal = DateTime.fromISO(dataIni, { zone }).startOf("day");
    const endLocal = DateTime.fromISO(dataFim, { zone }).endOf("day");

    const startUtc = startLocal.toUTC().toJSDate();
    const endUtc = endLocal.toUTC().toJSDate();

    const qb = this.ds
      .getRepository(HistoricoOcupacao)
      .createQueryBuilder("h")
      .leftJoinAndSelect("h.leito", "leito")
      .where("h.inicio >= :start AND h.inicio <= :end", {
        start: startUtc,
        end: endUtc,
      });

    if (unidadeId) qb.andWhere("h.unidadeId = :unidadeId", { unidadeId });
    if (hospitalId) qb.andWhere("h.hospitalId = :hospitalId", { hospitalId });

    qb.orderBy("h.inicio", "ASC");

    return qb.getMany();
  }

  async buscarPorId(id: string) {
    return this.ds.getRepository(HistoricoOcupacao).findOne({
      where: { id },
      relations: ["leito"],
    });
  }

  // Buscar quantos leitos estavam operacionais (não inativos) em uma data específica
  async contarLeitosOperacionaisPorDia(data: string, unidadeId: string) {
    const zone = "America/Sao_Paulo";
    const dataLocal = DateTime.fromISO(data, { zone });
    const startUtc = dataLocal.startOf("day").toUTC().toJSDate();
    const endUtc = dataLocal.endOf("day").toUTC().toJSDate();

    // Buscar todos os leitos da unidade que tiveram registro neste dia
    const historicos = await this.ds
      .getRepository(HistoricoOcupacao)
      .createQueryBuilder("h")
      .where("h.unidadeId = :unidadeId", { unidadeId })
      .andWhere("h.inicio <= :end", { end: endUtc })
      .andWhere("(h.fim IS NULL OR h.fim >= :start)", { start: startUtc })
      .getMany();

    // Contar leitos únicos que não estavam com status INATIVO
    const leitosOperacionais = new Set();

    for (const hist of historicos) {
      if (hist.leitoStatus && hist.leitoStatus !== "INATIVO") {
        leitosOperacionais.add(hist.leito?.id || hist.leitoNumero);
      }
    }

    return leitosOperacionais.size;
  }

  // Buscar quantos leitos totais estavam cadastrados em uma data específica para uma unidade
  async contarLeitosTotaisPorDia(data: string, unidadeId: string) {
    const zone = "America/Sao_Paulo";
    const dataLocal = DateTime.fromISO(data, { zone });
    const startUtc = dataLocal.startOf("day").toUTC().toJSDate();
    const endUtc = dataLocal.endOf("day").toUTC().toJSDate();

    // Buscar todos os leitos da unidade que tiveram registro neste dia
    const historicos = await this.ds
      .getRepository(HistoricoOcupacao)
      .createQueryBuilder("h")
      .where("h.unidadeId = :unidadeId", { unidadeId })
      .andWhere("h.inicio <= :end", { end: endUtc })
      .andWhere("(h.fim IS NULL OR h.fim >= :start)", { start: startUtc })
      .getMany();

    // Contar leitos únicos (independente do status)
    const leitosUnicos = new Set();

    for (const hist of historicos) {
      leitosUnicos.add(hist.leito?.id || hist.leitoNumero);
    }

    return leitosUnicos.size;
  }

  // Buscar quantos leitos estavam inativos em uma data específica para uma unidade
  async contarLeitosInativosPorDia(data: string, unidadeId: string) {
    const zone = "America/Sao_Paulo";
    const dataLocal = DateTime.fromISO(data, { zone });
    const startUtc = dataLocal.startOf("day").toUTC().toJSDate();
    const endUtc = dataLocal.endOf("day").toUTC().toJSDate();

    // Buscar todos os leitos da unidade que tiveram registro neste dia
    const historicos = await this.ds
      .getRepository(HistoricoOcupacao)
      .createQueryBuilder("h")
      .where("h.unidadeId = :unidadeId", { unidadeId })
      .andWhere("h.inicio <= :end", { end: endUtc })
      .andWhere("(h.fim IS NULL OR h.fim >= :start)", { start: startUtc })
      .getMany();

    // Contar leitos únicos que estavam com status INATIVO
    const leitosInativos = new Set();

    for (const hist of historicos) {
      if (hist.leitoStatus === "INATIVO") {
        leitosInativos.add(hist.leito?.id || hist.leitoNumero);
      }
    }

    return leitosInativos.size;
  }
}
