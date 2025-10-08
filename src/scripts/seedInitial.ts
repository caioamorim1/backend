import "reflect-metadata";
import dotenv from "dotenv";
import { AppDataSource } from "../ormconfig";
import { Rede } from "../entities/Rede";
import { Grupo } from "../entities/Grupo";
import { Regiao } from "../entities/Regiao";
import { Hospital } from "../entities/Hospital";
import { Cargo } from "../entities/Cargo";
import { UnidadeInternacao } from "../entities/UnidadeInternacao";
import { UnidadeNaoInternacao } from "../entities/UnidadeNaoInternacao";
import { Leito, StatusLeito } from "../entities/Leito";
import { SitioFuncional } from "../entities/SitioFuncional";
import { CargoUnidade } from "../entities/CargoUnidade";
import { CargoSitio } from "../entities/CargoSitio";
import { SitioDistribuicao } from "../entities/SitioDistribuicao";
import { Colaborador } from "../entities/Colaborador";
import { Admin } from "../entities/Admin";
import { ScpMetodo } from "../entities/ScpMetodo";
import { ParametrosUnidade } from "../entities/ParametrosUnidade";
import { ParametrosNaoInternacao } from "../entities/ParametrosNaoInternacao";
import {
  ClassificacaoCuidado,
  AvaliacaoSCP,
  StatusSessaoAvaliacao,
} from "../entities/AvaliacaoSCP";
import { HistoricoOcupacao } from "../entities/HistoricoOcupacao";
import bcrypt from "bcrypt";
import { DateTime } from "luxon";

dotenv.config({ path: ".env" });

const SALT_ROUNDS = 10;

async function seedDatabase() {
  console.log("🌱 Iniciando seed do banco de dados...");

  try {
    await AppDataSource.initialize();
    console.log("✅ Conexão com banco estabelecida");

    // ============================================================
    // 1. MÉTODOS SCP (independentes)
    // ============================================================
    console.log("\n📋 Criando métodos SCP...");
    const scpFugulin = AppDataSource.getRepository(ScpMetodo).create({
      key: "FUGULIN",
      title: "Método Fugulin",
      description: "Sistema de Classificação de Pacientes - Fugulin",
      questions: [
        {
          key: "estado_mental",
          text: "Estado mental",
          options: [
            { label: "Inconsciente", value: 4 },
            { label: "Períodos de inconsciência", value: 3 },
            {
              label: "Períodos de desorientação no espaço e no tempo",
              value: 2,
            },
            { label: "Orientação no tempo e espaço", value: 1 },
          ],
        },
        {
          key: "oxigenacao",
          text: "Oxigenação",
          options: [
            { label: "Ventilação Mecânica", value: 4 },
            { label: "Uso contínuo de máscara ou cateter de O2", value: 3 },
            { label: "Uso intermitente de máscara ou cateter de O2", value: 2 },
            { label: "Não dependente de oxigenioterapia", value: 1 },
          ],
        },
        {
          key: "sinais_vitais",
          text: "Sinais vitais",
          options: [
            { label: "Controle em intervalos ≤ 02h", value: 4 },
            { label: "Controle em intervalos de 04h", value: 3 },
            { label: "Controle em intervalos de 06h", value: 2 },
            { label: "Controle de rotina (8h)", value: 1 },
          ],
        },
        {
          key: "motilidade",
          text: "Motilidade",
          options: [
            { label: "Restrito ao leito", value: 4 },
            { label: "Locomoção com auxílio total", value: 3 },
            { label: "Locomoção com auxílio parcial", value: 2 },
            { label: "Deambula sem auxílio", value: 1 },
          ],
        },
        {
          key: "deambulacao",
          text: "Deambulação",
          options: [
            { label: "Incapaz", value: 4 },
            { label: "Auxílio total", value: 3 },
            { label: "Auxílio parcial", value: 2 },
            { label: "Autônomo", value: 1 },
          ],
        },
        {
          key: "alimentacao",
          text: "Alimentação",
          options: [
            { label: "Nutrição parenteral ou enteral", value: 4 },
            { label: "Por sonda", value: 3 },
            { label: "Com auxílio", value: 2 },
            { label: "Autônomo", value: 1 },
          ],
        },
        {
          key: "cuidado_corporal",
          text: "Cuidado corporal",
          options: [
            { label: "Banho no leito", value: 4 },
            { label: "Auxílio total", value: 3 },
            { label: "Auxílio parcial", value: 2 },
            { label: "Autônomo", value: 1 },
          ],
        },
        {
          key: "eliminacao",
          text: "Eliminação",
          options: [
            { label: "Dependente total", value: 4 },
            { label: "Uso de comadre/papagaio", value: 3 },
            { label: "Com auxílio", value: 2 },
            { label: "Autônomo", value: 1 },
          ],
        },
        {
          key: "terapeutica",
          text: "Terapêutica",
          options: [
            { label: "EV contínua ou múltipla", value: 4 },
            { label: "EV intermitente", value: 3 },
            { label: "IM, VO, SC", value: 2 },
            { label: "Nenhuma medicação", value: 1 },
          ],
        },
      ],
      faixas: [
        { min: 0, max: 14, classe: ClassificacaoCuidado.MINIMOS },
        { min: 15, max: 24, classe: ClassificacaoCuidado.INTERMEDIARIOS },
        { min: 25, max: 28, classe: ClassificacaoCuidado.ALTA_DEPENDENCIA },
        { min: 29, max: 34, classe: ClassificacaoCuidado.SEMI_INTENSIVOS },
        { min: 35, max: 9999, classe: ClassificacaoCuidado.INTENSIVOS },
      ],
    });
    await AppDataSource.getRepository(ScpMetodo).save(scpFugulin);
    console.log(`  ✓ Método SCP: ${scpFugulin.title}`);

    // ============================================================
    // 2. REDES
    // ============================================================
    console.log("\n🌐 Criando redes...");
    const rede1 = AppDataSource.getRepository(Rede).create({
      nome: "Rede Hospitalar São Paulo",
    });
    await AppDataSource.getRepository(Rede).save(rede1);
    console.log(`  ✓ Rede: ${rede1.nome}`);

    const rede2 = AppDataSource.getRepository(Rede).create({
      nome: "Rede Federal de Saúde",
    });
    await AppDataSource.getRepository(Rede).save(rede2);
    console.log(`  ✓ Rede: ${rede2.nome}`);

    // ============================================================
    // 3. GRUPOS (dependem de Rede)
    // ============================================================
    console.log("\n👥 Criando grupos...");
    const grupo1 = AppDataSource.getRepository(Grupo).create({
      nome: "Grupo Região Metropolitana",
      rede: rede1,
    });
    await AppDataSource.getRepository(Grupo).save(grupo1);
    console.log(`  ✓ Grupo: ${grupo1.nome}`);

    const grupo2 = AppDataSource.getRepository(Grupo).create({
      nome: "Grupo Interior",
      rede: rede1,
    });
    await AppDataSource.getRepository(Grupo).save(grupo2);
    console.log(`  ✓ Grupo: ${grupo2.nome}`);

    // ============================================================
    // 4. REGIÕES (dependem de Grupo)
    // ============================================================
    console.log("\n📍 Criando regiões...");
    const regiao1 = AppDataSource.getRepository(Regiao).create({
      nome: "Região Central",
      grupo: grupo1,
    });
    await AppDataSource.getRepository(Regiao).save(regiao1);
    console.log(`  ✓ Região: ${regiao1.nome}`);

    const regiao2 = AppDataSource.getRepository(Regiao).create({
      nome: "Região Oeste",
      grupo: grupo2,
    });
    await AppDataSource.getRepository(Regiao).save(regiao2);
    console.log(`  ✓ Região: ${regiao2.nome}`);

    // ============================================================
    // 5. HOSPITAIS (dependem de Região)
    // ============================================================
    console.log("\n🏥 Criando hospitais...");
    const hospital1 = AppDataSource.getRepository(Hospital).create({
      nome: "Hospital Geral de São Paulo",
      cnpj: "12.345.678/0001-90",
      endereco: "Av. Paulista, 1000 - São Paulo, SP",
      telefone: "(11) 3000-0000",
      regiao: regiao1,
    });
    await AppDataSource.getRepository(Hospital).save(hospital1);
    console.log(`  ✓ Hospital: ${hospital1.nome}`);

    const hospital2 = AppDataSource.getRepository(Hospital).create({
      nome: "Hospital Municipal de Campinas",
      cnpj: "98.765.432/0001-10",
      endereco: "Rua das Flores, 500 - Campinas, SP",
      telefone: "(19) 4000-0000",
      regiao: regiao2,
    });
    await AppDataSource.getRepository(Hospital).save(hospital2);
    console.log(`  ✓ Hospital: ${hospital2.nome}`);

    // ============================================================
    // 6. CARGOS (dependem de Hospital)
    // ============================================================
    console.log("\n💼 Criando cargos...");
    const cargoEnfermeiro1 = AppDataSource.getRepository(Cargo).create({
      nome: "Enfermeiro Assistencial",
      hospital: hospital1,
      salario: "4500,00",
      carga_horaria: "36",
      adicionais_tributos: "1200,00",
      descricao: "Enfermeiro responsável pela assistência direta ao paciente",
    });
    await AppDataSource.getRepository(Cargo).save(cargoEnfermeiro1);
    console.log(`  ✓ Cargo: ${cargoEnfermeiro1.nome} (${hospital1.nome})`);

    const cargoTecnico1 = AppDataSource.getRepository(Cargo).create({
      nome: "Técnico de Enfermagem",
      hospital: hospital1,
      salario: "2800,00",
      carga_horaria: "36",
      adicionais_tributos: "800,00",
      descricao: "Técnico de enfermagem para assistência ao paciente",
    });
    await AppDataSource.getRepository(Cargo).save(cargoTecnico1);
    console.log(`  ✓ Cargo: ${cargoTecnico1.nome} (${hospital1.nome})`);

    const cargoEnfermeiro2 = AppDataSource.getRepository(Cargo).create({
      nome: "Enfermeiro Plantonista",
      hospital: hospital2,
      salario: "4200,00",
      carga_horaria: "36",
      adicionais_tributos: "1100,00",
      descricao: "Enfermeiro para plantão",
    });
    await AppDataSource.getRepository(Cargo).save(cargoEnfermeiro2);
    console.log(`  ✓ Cargo: ${cargoEnfermeiro2.nome} (${hospital2.nome})`);

    const cargoTecnico2 = AppDataSource.getRepository(Cargo).create({
      nome: "Auxiliar de Enfermagem",
      hospital: hospital2,
      salario: "2500,00",
      carga_horaria: "40",
      adicionais_tributos: "700,00",
      descricao: "Auxiliar de enfermagem",
    });
    await AppDataSource.getRepository(Cargo).save(cargoTecnico2);
    console.log(`  ✓ Cargo: ${cargoTecnico2.nome} (${hospital2.nome})`);

    // ============================================================
    // 7. COLABORADORES (dependem de Hospital)
    // ============================================================
    console.log("\n👨‍⚕️ Criando colaboradores...");
    const senhaHash = await bcrypt.hash("senha123", SALT_ROUNDS);

    const colaborador1 = AppDataSource.getRepository(Colaborador).create({
      nome: "Dr. João Silva",
      email: "joao.silva@hospital1.com",
      cpf: "123.456.789-00",
      senha: senhaHash,
      mustChangePassword: false,
      permissao: "GESTOR",
      hospital: hospital1,
    });
    await AppDataSource.getRepository(Colaborador).save(colaborador1);
    console.log(`  ✓ Colaborador: ${colaborador1.nome} (${hospital1.nome})`);

    const colaborador2 = AppDataSource.getRepository(Colaborador).create({
      nome: "Enf. Maria Santos",
      email: "maria.santos@hospital1.com",
      cpf: "987.654.321-00",
      senha: senhaHash,
      mustChangePassword: false,
      permissao: "COMUM",
      hospital: hospital1,
    });
    await AppDataSource.getRepository(Colaborador).save(colaborador2);
    console.log(`  ✓ Colaborador: ${colaborador2.nome} (${hospital1.nome})`);

    const colaborador3 = AppDataSource.getRepository(Colaborador).create({
      nome: "Dr. Pedro Oliveira",
      email: "pedro.oliveira@hospital2.com",
      cpf: "111.222.333-44",
      senha: senhaHash,
      mustChangePassword: false,
      permissao: "COMUM",
      hospital: hospital2,
    });
    await AppDataSource.getRepository(Colaborador).save(colaborador3);
    console.log(`  ✓ Colaborador: ${colaborador3.nome} (${hospital2.nome})`);

    // ============================================================
    // 8. ADMINISTRADORES (independentes ou com grupos futuros)
    // ============================================================
    console.log("\n🔐 Criando administradores...");
    const admin1 = AppDataSource.getRepository(Admin).create({
      nome: "Admin Master",
      email: "admin@dimensiona.com",
      senha: await bcrypt.hash("admin123", SALT_ROUNDS),
    });
    await AppDataSource.getRepository(Admin).save(admin1);
    console.log(`  ✓ Admin: ${admin1.nome}`);

    // ============================================================
    // 9. UNIDADES DE INTERNAÇÃO (dependem de Hospital e ScpMetodo)
    // ============================================================
    console.log("\n🏨 Criando unidades de internação...");
    const unidadeUTI = AppDataSource.getRepository(UnidadeInternacao).create({
      nome: "UTI Adulto",
      hospital: hospital1,
      horas_extra_reais: "500,00",
      horas_extra_projetadas: "400,00",
      scpMetodo: scpFugulin,
    });
    await AppDataSource.getRepository(UnidadeInternacao).save(unidadeUTI);
    console.log(`  ✓ Unidade: ${unidadeUTI.nome} (${hospital1.nome})`);

    const unidadeEnfermaria = AppDataSource.getRepository(
      UnidadeInternacao
    ).create({
      nome: "Enfermaria Clínica",
      hospital: hospital1,
      horas_extra_reais: "300,00",
      horas_extra_projetadas: "250,00",
      scpMetodo: scpFugulin,
    });
    await AppDataSource.getRepository(UnidadeInternacao).save(
      unidadeEnfermaria
    );
    console.log(`  ✓ Unidade: ${unidadeEnfermaria.nome} (${hospital1.nome})`);

    const unidadePS = AppDataSource.getRepository(UnidadeInternacao).create({
      nome: "Pronto Socorro",
      hospital: hospital2,
      horas_extra_reais: "600,00",
      horas_extra_projetadas: "500,00",
      scpMetodo: scpFugulin,
    });
    await AppDataSource.getRepository(UnidadeInternacao).save(unidadePS);
    console.log(`  ✓ Unidade: ${unidadePS.nome} (${hospital2.nome})`);

    // ============================================================
    // 10. LEITOS (dependem de UnidadeInternacao)
    // ============================================================
    console.log("\n🛏️ Criando leitos...");
    const leitos: Leito[] = [];

    // Leitos UTI
    for (let i = 1; i <= 10; i++) {
      const leito = AppDataSource.getRepository(Leito).create({
        numero: `UTI-${String(i).padStart(2, "0")}`,
        unidade: unidadeUTI,
        status: StatusLeito.PENDENTE,
      });
      leitos.push(leito);
    }

    // Leitos Enfermaria
    for (let i = 1; i <= 20; i++) {
      const leito = AppDataSource.getRepository(Leito).create({
        numero: `ENF-${String(i).padStart(2, "0")}`,
        unidade: unidadeEnfermaria,
        status: StatusLeito.PENDENTE,
      });
      leitos.push(leito);
    }

    // Leitos PS
    for (let i = 1; i <= 15; i++) {
      const leito = AppDataSource.getRepository(Leito).create({
        numero: `PS-${String(i).padStart(2, "0")}`,
        unidade: unidadePS,
        status: StatusLeito.PENDENTE,
      });
      leitos.push(leito);
    }

    await AppDataSource.getRepository(Leito).save(leitos);
    console.log(`  ✓ ${leitos.length} leitos criados`);

    // ============================================================
    // 11. CARGOS POR UNIDADE (CargoUnidade)
    // ============================================================
    console.log("\n📊 Vinculando cargos às unidades de internação...");
    const cargoUnidade1 = AppDataSource.getRepository(CargoUnidade).create({
      cargo: cargoEnfermeiro1,
      unidade: unidadeUTI,
      quantidade_funcionarios: 12,
    });
    await AppDataSource.getRepository(CargoUnidade).save(cargoUnidade1);

    const cargoUnidade2 = AppDataSource.getRepository(CargoUnidade).create({
      cargo: cargoTecnico1,
      unidade: unidadeUTI,
      quantidade_funcionarios: 18,
    });
    await AppDataSource.getRepository(CargoUnidade).save(cargoUnidade2);

    const cargoUnidade3 = AppDataSource.getRepository(CargoUnidade).create({
      cargo: cargoEnfermeiro1,
      unidade: unidadeEnfermaria,
      quantidade_funcionarios: 8,
    });
    await AppDataSource.getRepository(CargoUnidade).save(cargoUnidade3);

    const cargoUnidade4 = AppDataSource.getRepository(CargoUnidade).create({
      cargo: cargoTecnico1,
      unidade: unidadeEnfermaria,
      quantidade_funcionarios: 12,
    });
    await AppDataSource.getRepository(CargoUnidade).save(cargoUnidade4);

    console.log("  ✓ Cargos vinculados às unidades de internação");

    // ============================================================
    // 12. PARÂMETROS DAS UNIDADES DE INTERNAÇÃO
    // ============================================================
    console.log("\n⚙️ Criando parâmetros de dimensionamento...");
    const paramUTI = AppDataSource.getRepository(ParametrosUnidade).create({
      unidade: unidadeUTI,
      ist: 15,
      aplicarIST: true,
      diasSemana: 7,
    });
    await AppDataSource.getRepository(ParametrosUnidade).save(paramUTI);
    console.log(`  ✓ Parâmetros: ${unidadeUTI.nome}`);

    const paramEnfermaria = AppDataSource.getRepository(
      ParametrosUnidade
    ).create({
      unidade: unidadeEnfermaria,
      ist: 15,
      aplicarIST: true,
      diasSemana: 6,
    });
    await AppDataSource.getRepository(ParametrosUnidade).save(paramEnfermaria);
    console.log(`  ✓ Parâmetros: ${unidadeEnfermaria.nome}`);

    // ============================================================
    // 13. UNIDADES DE NÃO INTERNAÇÃO
    // ============================================================
    console.log("\n🏢 Criando unidades de não internação...");
    const unidadeAmbulatorio = AppDataSource.getRepository(
      UnidadeNaoInternacao
    ).create({
      nome: "Ambulatório Oncológico",
      hospital: hospital1,
      horas_extra_reais: "200,00",
      horas_extra_projetadas: "150,00",
    });
    await AppDataSource.getRepository(UnidadeNaoInternacao).save(
      unidadeAmbulatorio
    );
    console.log(
      `  ✓ Unidade NI: ${unidadeAmbulatorio.nome} (${hospital1.nome})`
    );

    const unidadeSADT = AppDataSource.getRepository(
      UnidadeNaoInternacao
    ).create({
      nome: "SADT - Exames",
      hospital: hospital2,
      horas_extra_reais: "150,00",
      horas_extra_projetadas: "100,00",
    });
    await AppDataSource.getRepository(UnidadeNaoInternacao).save(unidadeSADT);
    console.log(`  ✓ Unidade NI: ${unidadeSADT.nome} (${hospital2.nome})`);

    // ============================================================
    // 14. SÍTIOS FUNCIONAIS (dependem de UnidadeNaoInternacao)
    // ============================================================
    console.log("\n🏗️ Criando sítios funcionais...");
    const sitio1 = AppDataSource.getRepository(SitioFuncional).create({
      nome: "Consultório 1",
      unidade: unidadeAmbulatorio,
    });
    await AppDataSource.getRepository(SitioFuncional).save(sitio1);
    console.log(`  ✓ Sítio: ${sitio1.nome}`);

    const sitio2 = AppDataSource.getRepository(SitioFuncional).create({
      nome: "Sala de Quimioterapia",
      unidade: unidadeAmbulatorio,
    });
    await AppDataSource.getRepository(SitioFuncional).save(sitio2);
    console.log(`  ✓ Sítio: ${sitio2.nome}`);

    const sitio3 = AppDataSource.getRepository(SitioFuncional).create({
      nome: "Sala de RX",
      unidade: unidadeSADT,
    });
    await AppDataSource.getRepository(SitioFuncional).save(sitio3);
    console.log(`  ✓ Sítio: ${sitio3.nome}`);

    // ============================================================
    // 15. CARGOS POR SÍTIO (CargoSitio)
    // ============================================================
    console.log("\n💼 Vinculando cargos aos sítios...");
    const cargoSitio1 = AppDataSource.getRepository(CargoSitio).create({
      sitio: sitio1,
      cargoUnidade: cargoUnidade1, // Enfermeiro
      quantidade_funcionarios: 2,
    });
    await AppDataSource.getRepository(CargoSitio).save(cargoSitio1);

    const cargoSitio2 = AppDataSource.getRepository(CargoSitio).create({
      sitio: sitio2,
      cargoUnidade: cargoUnidade1,
      quantidade_funcionarios: 3,
    });
    await AppDataSource.getRepository(CargoSitio).save(cargoSitio2);

    const cargoSitio3 = AppDataSource.getRepository(CargoSitio).create({
      sitio: sitio2,
      cargoUnidade: cargoUnidade2, // Técnico
      quantidade_funcionarios: 4,
    });
    await AppDataSource.getRepository(CargoSitio).save(cargoSitio3);
    console.log("  ✓ Cargos vinculados aos sítios");

    // ============================================================
    // 16. DISTRIBUIÇÕES (dependem de SitioFuncional)
    // ============================================================
    console.log("\n📅 Criando distribuições de turnos...");
    const distrib1 = AppDataSource.getRepository(SitioDistribuicao).create({
      sitio: sitio1,
      categoria: "ENF",
      segSexManha: 1,
      segSexTarde: 1,
      segSexNoite1: 0,
      segSexNoite2: 0,
      sabDomManha: 1,
      sabDomTarde: 0,
      sabDomNoite1: 0,
      sabDomNoite2: 0,
    });
    await AppDataSource.getRepository(SitioDistribuicao).save(distrib1);

    const distrib2 = AppDataSource.getRepository(SitioDistribuicao).create({
      sitio: sitio2,
      categoria: "ENF",
      segSexManha: 2,
      segSexTarde: 2,
      segSexNoite1: 0,
      segSexNoite2: 0,
      sabDomManha: 0,
      sabDomTarde: 0,
      sabDomNoite1: 0,
      sabDomNoite2: 0,
    });
    await AppDataSource.getRepository(SitioDistribuicao).save(distrib2);

    const distrib3 = AppDataSource.getRepository(SitioDistribuicao).create({
      sitio: sitio2,
      categoria: "TEC",
      segSexManha: 3,
      segSexTarde: 3,
      segSexNoite1: 0,
      segSexNoite2: 0,
      sabDomManha: 0,
      sabDomTarde: 0,
      sabDomNoite1: 0,
      sabDomNoite2: 0,
    });
    await AppDataSource.getRepository(SitioDistribuicao).save(distrib3);
    console.log("  ✓ Distribuições de turnos criadas");

    // ============================================================
    // 17. PARÂMETROS UNIDADES NÃO INTERNAÇÃO
    // ============================================================
    console.log("\n⚙️ Criando parâmetros para unidades NI...");
    const paramNI = AppDataSource.getRepository(ParametrosNaoInternacao).create(
      {
        unidade: unidadeAmbulatorio,
        jornadaSemanalEnfermeiro: 36,
        jornadaSemanalTecnico: 40,
        indiceSegurancaTecnica: 0.15,
        equipeComRestricao: false,
        diasFuncionamentoMensal: 26,
        diasSemana: 6,
      }
    );
    await AppDataSource.getRepository(ParametrosNaoInternacao).save(paramNI);
    console.log(`  ✓ Parâmetros NI: ${unidadeAmbulatorio.nome}`);

    // ========================================
    // 11. CRIAR AVALIAÇÕES E HISTÓRICO DE OCUPAÇÃO
    // ========================================
    console.log("\n📊 Criando avaliações e histórico de ocupação...");

    const hoje = DateTime.now().setZone("America/Sao_Paulo");
    const avaliacoesRepo = AppDataSource.getRepository(AvaliacaoSCP);
    const historicoRepo = AppDataSource.getRepository(HistoricoOcupacao);

    // Array de colaboradores para sortear autores
    const colaboradoresArray = [colaborador1, colaborador2, colaborador3];

    // Função auxiliar para gerar pontuação aleatória baseada na classificação
    const gerarPontuacao = (
      classificacao: ClassificacaoCuidado
    ): { itens: Record<string, number>; total: number } => {
      const fugulinQuestions = scpFugulin.questions as any[];
      const itens: Record<string, number> = {};
      let total = 0;

      // Define faixas de pontos por questão baseado na classificação desejada
      const ranges: Record<ClassificacaoCuidado, [number, number]> = {
        [ClassificacaoCuidado.MINIMOS]: [1, 2],
        [ClassificacaoCuidado.INTERMEDIARIOS]: [2, 3],
        [ClassificacaoCuidado.ALTA_DEPENDENCIA]: [3, 4],
        [ClassificacaoCuidado.SEMI_INTENSIVOS]: [3, 4],
        [ClassificacaoCuidado.INTENSIVOS]: [4, 4],
      };

      const [min, max] = ranges[classificacao];

      fugulinQuestions.forEach((q) => {
        const pontos = Math.floor(Math.random() * (max - min + 1)) + min;
        itens[q.id] = pontos;
        total += pontos;
      });

      return { itens, total };
    };

    // Distribuição de classificações por tipo de unidade
    const classificacoesUTI = [
      ClassificacaoCuidado.SEMI_INTENSIVOS,
      ClassificacaoCuidado.INTENSIVOS,
      ClassificacaoCuidado.ALTA_DEPENDENCIA,
    ];

    const classificacoesEnfermaria = [
      ClassificacaoCuidado.MINIMOS,
      ClassificacaoCuidado.INTERMEDIARIOS,
      ClassificacaoCuidado.ALTA_DEPENDENCIA,
    ];

    const classificacoesPS = [
      ClassificacaoCuidado.INTERMEDIARIOS,
      ClassificacaoCuidado.ALTA_DEPENDENCIA,
      ClassificacaoCuidado.SEMI_INTENSIVOS,
    ];

    // Criar avaliações para cada leito nos últimos 30 dias
    let totalAvaliacoes = 0;
    let totalHistorico = 0;

    for (const leito of leitos) {
      // Determinar possíveis classificações baseado na unidade
      let classificacoesPossiveis: ClassificacaoCuidado[];

      if (leito.unidade.id === unidadeUTI.id) {
        classificacoesPossiveis = classificacoesUTI;
      } else if (leito.unidade.id === unidadeEnfermaria.id) {
        classificacoesPossiveis = classificacoesEnfermaria;
      } else {
        classificacoesPossiveis = classificacoesPS;
      }

      // Criar de 3 a 8 avaliações por leito nos últimos 30 dias
      const numAvaliacoes = Math.floor(Math.random() * 6) + 3;

      for (let i = 0; i < numAvaliacoes; i++) {
        // Data aleatória nos últimos 30 dias
        const diasAtras = Math.floor(Math.random() * 30);
        const dataAvaliacao = hoje.minus({ days: diasAtras });

        // Escolher classificação aleatória
        const classificacao =
          classificacoesPossiveis[
            Math.floor(Math.random() * classificacoesPossiveis.length)
          ];

        // Gerar pontuação
        const { itens, total } = gerarPontuacao(classificacao);

        // Escolher autor aleatório
        const autor =
          colaboradoresArray[
            Math.floor(Math.random() * colaboradoresArray.length)
          ];

        // Criar avaliação SCP (sessão)
        const avaliacao = avaliacoesRepo.create({
          dataAplicacao: dataAvaliacao.toFormat("yyyy-MM-dd"),
          unidade: leito.unidade,
          leito: leito,
          autor: autor,
          scp: "Fugulin",
          prontuario: `PRONT-${Math.floor(Math.random() * 90000) + 10000}`,
          itens: itens,
          totalPontos: total,
          classificacao: classificacao,
          statusSessao: StatusSessaoAvaliacao.LIBERADA,
          expiresAt: null,
        });
        await avaliacoesRepo.save(avaliacao);
        totalAvaliacoes++;

        // Criar histórico de ocupação correspondente
        const duracaoHoras = Math.floor(Math.random() * 12) + 6; // 6 a 18 horas
        const inicio = dataAvaliacao
          .set({ hour: 8, minute: 0, second: 0 })
          .toJSDate();
        const fim = dataAvaliacao
          .set({ hour: 8 + duracaoHoras, minute: 0, second: 0 })
          .toJSDate();

        const historico = historicoRepo.create({
          leito: leito,
          unidadeId: leito.unidade.id,
          hospitalId: leito.unidade.hospital.id,
          leitoNumero: leito.numero,
          leitoStatus: StatusLeito.ATIVO,
          scp: "Fugulin",
          totalPontos: total,
          classificacao: classificacao,
          itens: itens,
          autorId: autor.id,
          autorNome: autor.nome,
          inicio: inicio,
          fim: fim,
        });
        await historicoRepo.save(historico);
        totalHistorico++;
      }
    }

    console.log(`  ✓ ${totalAvaliacoes} avaliações SCP criadas`);
    console.log(
      `  ✓ ${totalHistorico} registros de histórico de ocupação criados`
    );

    console.log("\n✅ Seed concluído com sucesso!");
    console.log("\n📊 Resumo:");
    console.log(`  • 2 Redes`);
    console.log(`  • 2 Grupos`);
    console.log(`  • 2 Regiões`);
    console.log(`  • 2 Hospitais`);
    console.log(`  • 4 Cargos`);
    console.log(`  • 3 Colaboradores`);
    console.log(`  • 1 Administrador`);
    console.log(`  • 1 Método SCP (Fugulin)`);
    console.log(`  • 3 Unidades de Internação`);
    console.log(`  • ${leitos.length} Leitos`);
    console.log(`  • 2 Unidades de Não Internação`);
    console.log(`  • 3 Sítios Funcionais`);
    console.log(`  • ${totalAvaliacoes} Avaliações SCP (últimos 30 dias)`);
    console.log(`  • ${totalHistorico} Registros de histórico de ocupação`);
    console.log(`  • Cargos, distribuições e parâmetros configurados`);
    console.log("\n🔐 Credenciais de acesso:");
    console.log(`  Admin: admin@dimensiona.com / admin123`);
    console.log(`  Colaborador: joao.silva@hospital1.com / senha123`);
  } catch (error) {
    console.error("\n❌ Erro durante o seed:", error);
    process.exitCode = 1;
  } finally {
    await AppDataSource.destroy();
  }
}

if (require.main === module) {
  seedDatabase().finally(() => {
    if (process.exitCode && process.exitCode !== 0) {
      process.exit(process.exitCode);
    }
  });
}

export default seedDatabase;
