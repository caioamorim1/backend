import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  ManyToOne,
  OneToMany,
} from "typeorm";
import { Hospital } from "./Hospital";
import { Leito } from "./Leito";
import { ScpMetodo } from "./ScpMetodo";
import { CargoUnidade } from "./CargoUnidade";
import { Coleta } from "./Coleta";

@Entity("unidades_internacao")
export class UnidadeInternacao {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @ManyToOne(() => Hospital, (h) => h.unidades, {
    nullable: false,
    onDelete: "CASCADE",
  })
  hospital!: Hospital;

  @Column({ type: "varchar", length: 120 })
  nome!: string;

  @Column({ nullable: true })
  descricao?: string;

  @OneToMany(() => Leito, (l) => l.unidade, { cascade: true, nullable: false })
  leitos!: Leito[];

  @OneToMany(() => Coleta, (coleta) => coleta.unidade)
  coletas?: Coleta[];

  @Column({ type: "varchar", length: 255, nullable: true })
  horas_extra_reais?: string;

  @Column({ type: "varchar", length: 255, nullable: true })
  horas_extra_projetadas?: string;

  @OneToMany(() => CargoUnidade, (cargoUnidade) => cargoUnidade.unidade, {
    cascade: true,
  })
  cargosUnidade!: CargoUnidade[];

  @ManyToOne(() => ScpMetodo, (m) => m.unidades, {
    nullable: true,
    onDelete: "SET NULL",
  })
  scpMetodo?: ScpMetodo | null;

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
