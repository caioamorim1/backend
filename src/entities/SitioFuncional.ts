import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  ManyToOne,
  OneToMany,
} from "typeorm";
import { UnidadeNaoInternacao } from "./UnidadeNaoInternacao";
import { CargoSitio } from "./CargoSitio";
// FormularioColeta association removed per new domain rules
import { Coleta } from "./Coleta";
import { SitioDistribuicao } from "./SitioDistribuicao";

@Entity("sitios_funcionais")
export class SitioFuncional {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @ManyToOne(() => UnidadeNaoInternacao, (u) => u.sitiosFuncionais, {
    nullable: false,
    onDelete: "CASCADE",
  })
  unidade!: UnidadeNaoInternacao;

  @Column({ type: "varchar", length: 120, nullable: true })
  nome?: string;

  @Column({ type: "text", nullable: true })
  descricao?: string;

  @Column({ type: "varchar", length: 20, default: "ativo" })
  status!: string; // 'ativo' | 'inativo'

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;

  @OneToMany(() => CargoSitio, (cs) => cs.sitio, { cascade: false })
  cargosSitio?: CargoSitio[];

  @OneToMany(() => Coleta, (coleta) => coleta.sitio)
  coletas?: Coleta[];

  @OneToMany(() => SitioDistribuicao, (dist) => dist.sitio, {
    cascade: true,
  })
  distribuicoes?: SitioDistribuicao[];
}
