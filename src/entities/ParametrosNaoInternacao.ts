import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  CreateDateColumn,
  UpdateDateColumn,
} from "typeorm";
import { UnidadeNaoInternacao } from "./UnidadeNaoInternacao";

@Entity("parametros_nao_internacao")
export class ParametrosNaoInternacao {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @ManyToOne(() => UnidadeNaoInternacao, {
    nullable: false,
    onDelete: "CASCADE",
  })
  unidade!: UnidadeNaoInternacao;

  @Column({ type: "varchar", length: 255, nullable: true })
  nome_enfermeiro?: string;

  @Column({ type: "varchar", length: 50, nullable: true })
  numero_coren?: string;

  @Column({ type: "int", nullable: true })
  jornadaSemanalEnfermeiro?: number;

  @Column({ type: "int", nullable: true })
  jornadaSemanalTecnico?: number;

  @Column({ type: "decimal", precision: 5, scale: 4, default: 0 })
  indiceSegurancaTecnica!: number;

  @Column({ type: "boolean", default: false })
  equipeComRestricao!: boolean;

  @Column({ type: "int", default: 30 })
  diasFuncionamentoMensal!: number;

  @Column({ type: "int", default: 7 })
  diasSemana!: number;

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
