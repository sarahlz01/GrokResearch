import json


def translate(raw_path: str):
    with open(raw_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
            
    out_list = []

    for conversation in raw:
        tweets = conversation.get("tweets")[::-1]
        thread_list = []
        for i in range(len(tweets)):
            thread_list.append(walk_backward(tweets[i:], tweets[-1].get("id")))
            
        
        # prune duplicate or too similar threads
        thread_list = prune_duplicate_threads(thread_list)
        # append to final list
        out_list.append(thread_list)
    
    #print
    thread_counter = 0
    for thread in out_list:
        print("---------------------------------------")
        for tweets, missing_parent in thread:
            thread_counter += 1
            for tweet in tweets:
                print("***" + tweet + "***")
        print("---------------------------------------\n\n\n")
    print(thread)

# accepts (array: threads, bool: missing_tweets, set: thread_set)
# returns a pair (array: threads, bool: missing_tweets)
def prune_duplicate_threads(thread_list):
    res = []
    
    i = 0
    j = 0
    while i < len(thread_list):
        while j < len(thread_list):
            if thread_list[i][2].issubset(thread_list[j][2]): # if thread 'i' is a subset of thread 'j', 'i' is smaller than 'j', so add 'j' and prune 'i'
                res.append((thread_list[j][0], thread_list[j][1]))
            elif thread_list[j][2].issubset(thread_list[i][2]): # vice versa
                res.append((thread_list[i][0], thread_list[i][1]))
            else: # if neither are subsets, add them both and skip over them
                res.append((thread_list[i][0], thread_list[i][1]))
            j += 1
        i += 1
        j = 0 
                
    return res


# accepts array: tweets
# returns a pair (array: threads, bool: missing_tweets, set: thread_set)
def walk_backward(tweets, firstTweetInConversation):
    thread_set = set() 
    curr = tweets[0]
    l = len(tweets)
    index = 0
    currReplyId = curr.get("inReplyToId")
    res = [curr.get("text")]
    thread_set.add(curr.get("id"))
    missing_tweets = True # if we are not able to find a matching replyToId --> ID, then we have a missing tweet
    while currReplyId != firstTweetInConversation and index < l:
        curr = tweets[index]
        if (curr.get("id") == currReplyId):
            thread_set.add(curr.get("id"))
            missing_tweets = False
            res.append(curr.get("text"))
            currReplyId = curr.get("inReplyToId")
        
        index += 1
    res.append(tweets[-1].get("text")) # get the first tweet in a thread
    return (res[::-1], missing_tweets, thread_set)

translate("./raw.json")