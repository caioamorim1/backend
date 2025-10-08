import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  JoinColumn,
  CreateDateColumn,
  UpdateDateColumn,
} from "typeorm";
import { UnidadeInternacao } from "./UnidadeInternacao";

@Entity("dimensionamento_unidade")
export class DimensionamentoUnidade {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @Column({ name: "unidade_id" })
  unidadeId!: string;

  @ManyToOne(() => UnidadeInternacao, { onDelete: "CASCADE" })
  @JoinColumn({ name: "unidade_id" })
  unidade!: UnidadeInternacao;

  @Column({
    name: "enfermeiro_cargo_horario",
    type: "decimal",
    precision: 10,
    scale: 2,
  })
  enfermeiroCargoHorario!: number;

  @Column({
    name: "enfermeiro_percentual_equipe",
    type: "decimal",
    precision: 5,
    scale: 4,
  })
  enfermeiroPercentualEquipe!: number;

  @Column({
    name: "tecnico_enfermagem_cargo_horario",
    type: "decimal",
    precision: 10,
    scale: 2,
  })
  tecnicoEnfermagemCargoHorario!: number;

  @Column({
    name: "tecnico_enfermagem_percentual_equipe",
    type: "decimal",
    precision: 5,
    scale: 4,
  })
  tecnicoEnfermagemPercentualEquipe!: number;

  @Column({ name: "indice_tecnico", type: "decimal", precision: 10, scale: 2 })
  indiceTecnico!: number;

  @Column({ name: "idade_equipe_restricoes", type: "varchar", length: 3 })
  idadeEquipeRestricoes!: "sim" | "nao";

  @Column({ name: "quantidade_leitos", type: "int" })
  quantidadeLeitos!: number;

  @Column({ name: "taxa_ocupacao", type: "decimal", precision: 5, scale: 2 })
  taxaOcupacao!: number;

  @Column({ name: "pcm", type: "decimal", precision: 10, scale: 2 })
  pcm!: number;

  @Column({ name: "pci", type: "decimal", precision: 10, scale: 2 })
  pci!: number;

  @Column({ name: "pcad", type: "decimal", precision: 10, scale: 2 })
  pcad!: number;

  @Column({ name: "pcsi", type: "decimal", precision: 10, scale: 2 })
  pcsi!: number;

  @Column({ name: "pcit", type: "decimal", precision: 10, scale: 2 })
  pcit!: number;

  @CreateDateColumn({ name: "created_at" })
  createdAt!: Date;

  @UpdateDateColumn({ name: "updated_at" })
  updatedAt!: Date;
}
