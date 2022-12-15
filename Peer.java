/*

Codigo de acordo com o tutorial abaixo. Favor ler para compreender melhor o funcionamento da aplicação.

http://www.jgroups.org/tutorial4/index.html

*/

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.util.Util;

import util.Mensagem;

import java.io.*;
import java.util.List;
import java.util.LinkedList;
import java.util.Scanner;

public class Peer extends ReceiverAdapter {
    JChannel channel;
    String user_name=System.getProperty("user.name", "n/a");
    final List<String> state=new LinkedList<>();
    View viewAtual;

    static final long TOTAL_NUMEROS = 100000;
    private long intervaloAtual;
    private long tempoCalculo, tempoTotalPeer, tempoTotal;

    public Peer () {
        this.intervaloAtual = 0;
    }

    private void numerosPrimos (long inicio, long fim) {
        boolean primo;

        System.out.println("Iniciando os cálculos entre " + inicio + " e " + fim);
        tempoCalculo = System.currentTimeMillis();

        if (fim < inicio) {
            long aux = inicio;
            inicio = fim;
            fim = aux;
        }
        if (inicio % 2 == 0)
            inicio++;

        for (long i = inicio; i <= fim; i+=2) {
            primo = true;
            for (long j = i-1; j > 1; j--) {
                if (i % j == 0) {
                    primo = false;
                    break;
                }
            }
            if (primo) {
                try {
                    Mensagem men = new Mensagem("PRIMO");
                    men.setParam("numero", String.valueOf(i));
                    Message mensagem = new Message(viewAtual.getCoord(), men);
                    channel.send(mensagem);
                } catch (Exception e){
                    System.out.println(e.getMessage());
                }
            }
        }
        tempoCalculo =  System.currentTimeMillis() - tempoCalculo;
        System.out.println("Fim dos cálculos entre " + inicio + " e " + fim + " - tempo de execução: " + tempoCalculo + "ms \n");

        solicitaIntervaloPrimos();
    }

    private long[] intervalo () {
        long inicio = 0, fim = 0;
        inicio = intervaloAtual+1;
        intervaloAtual += 1000;
        if (intervaloAtual >= TOTAL_NUMEROS) {
            intervaloAtual = TOTAL_NUMEROS;
        }
        fim = intervaloAtual;
        return new long[] {inicio, fim};
    }

    private void enviaMensagem (Message m) {
        try {
            channel.send(m);
        } catch (Exception e) {
            System.out.println(e.getMessage() + " - " + e.getClass());
        }
    }

    public void viewAccepted(View new_view) {
        viewAtual = new_view;
    }

    public void receive(Message msg) {
        Mensagem m = (Mensagem) msg.getObject();
        if (this.channel.getAddress() == viewAtual.getCoord()) {
            if (m.getOperacao().equals("SOLICITA")) {
                if (intervaloAtual >= TOTAL_NUMEROS) {
                    Mensagem men = new Mensagem("ENCERRA");
                    Message mensagem = new Message(msg.getSrc(), men);
                    enviaMensagem(mensagem);
                    if (this.viewAtual.getMembers().size() <= 2) {
                        tempoTotal = System.currentTimeMillis() - tempoTotal;
                        System.out.println("\nTempo total de execução:  " + tempoTotal + "\n");
                        channel.close();
                    }
                } else {
                    long[] intervalo = intervalo();
                    Mensagem men = new Mensagem("CALCULO");
                    men.setParam("inicio", String.valueOf(intervalo[0]));
                    men.setParam("fim", String.valueOf(intervalo[1]));
                    Message mensagem = new Message(msg.getSrc(), men);
                    enviaMensagem(mensagem);
                }
            } else if (m.getOperacao().equals("PRIMO")) {
                long primo = Integer.parseInt(m.getParam("numero"));
                System.out.println(msg.getSrc() + " enviou número primo: " + primo);
            }
        } else {
            if ((m.getOperacao().equals("AUTORIZA"))) {
                tempoTotalPeer = System.currentTimeMillis();
                solicitaIntervaloPrimos();
            } else if (m.getOperacao().equals("CALCULO")) {
                long inicio = Integer.parseInt(m.getParam("inicio"));
                long fim = Integer.parseInt(m.getParam("fim"));
                numerosPrimos(inicio, fim);
            } else if (m.getOperacao().equals("ENCERRA")) {
                tempoTotalPeer = System.currentTimeMillis() - tempoTotalPeer;
                System.out.println("\nTempo total de execução desse Peer: " + tempoTotalPeer + "\n");
                channel.close();
            }
        }

        synchronized(state) {
            state.add(msg.toString());
        }
    }

    public void getState(OutputStream output) throws Exception {
        synchronized(state) {
            Util.objectToStream(state, new DataOutputStream(output));
        }
    }

    @SuppressWarnings("unchecked")
    public void setState(InputStream input) throws Exception {
        List<String> list=Util.objectFromStream(new DataInputStream(input));
        synchronized(state) {
            state.clear();
            state.addAll(list);
        }
        System.out.println("received state (" + list.size() + " messages in chat history):");
        list.forEach(System.out::println);
    }

    private void start() throws Exception {
        channel=new JChannel().setReceiver(this);
        channel.connect("ChatCluster");
        channel.getState(null, 10000);
        inicio();
    }

    private void inicio() {
        if (this.channel.getAddress() == viewAtual.getCoord()) {
            System.out.println("Sou cordenador");
            Scanner sc= new Scanner(System.in);
            sc.nextLine();
            tempoTotal = System.currentTimeMillis();
            Mensagem m = new Mensagem("AUTORIZA");
            Message msg = new Message(null, m);
            enviaMensagem(msg);
        }
    }

    private void solicitaIntervaloPrimos () {
        if (this.channel.getAddress() != viewAtual.getCoord()) {
            Mensagem m = new Mensagem("SOLICITA");
            Message msg = new Message(viewAtual.getCoord(), m);
            enviaMensagem(msg);
        }
    }

    public static void main(String[] args) throws Exception {
        new Peer().start();
    }
}